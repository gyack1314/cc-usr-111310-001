"""按键归并读取与在线压实的完整测试。

覆盖：put/patch/delete 严格总序语义、显式 null 与缺席字段、未生效 patch 不复活、
同序重放/冲突、有界多路归并与超大键组落盘、压实覆盖证明、墓碑消去条件、
提交围栏与并发追加、压实前后全部受保护 visible_seq 读取等价。
"""
import hashlib
import json
import shutil
import tempfile
import unittest
from pathlib import Path

from warehouse_data.table import (
    ABSENT,
    DELETED,
    LIVE,
    HandleBudget,
    MergeConflict,
    TableError,
    canonical,
    read_snapshot,
    validate_manifest,
    validate_record,
)
from warehouse_data.compaction import (
    CommitConflict,
    execute_compaction,
    plan_compaction,
)

SCHEMA = ["station", "temperature"]


def rec(key, seq, op, value=None, ordinal=0):
    out = {"key": key, "seq": seq, "ordinal": ordinal, "op": op}
    if value is not None:
        out["value"] = value
    return out


def put(key, seq, station, temperature, ordinal=0):
    return rec(key, seq, "put", {"station": station, "temperature": temperature}, ordinal)


def write_jsonl(path: Path, rows: list[dict]) -> dict:
    raw = ("".join(json.dumps(r, ensure_ascii=False, separators=(",", ":")) + "\n"
                   for r in rows)).encode("utf-8")
    path.write_bytes(raw)
    return {"path": path.name, "bytes": len(raw),
            "sha256": hashlib.sha256(raw).hexdigest(), "rows": len(rows)}


class LakeBuilder:
    """在临时目录里合成一个湖表清单与 JSONL 文件。"""

    def __init__(self, root: Path, max_open_files=4, memory_bytes=4096):
        self.root = root
        self.files = []
        self.rows = {}
        self.max_open_files = max_open_files
        self.memory_bytes = memory_bytes

    def add(self, fid: str, rows: list[dict]):
        self.rows[fid] = rows
        asset = write_jsonl(self.root / f"{fid}.jsonl", rows)
        self.files.append({"id": fid, "asset": asset})
        return self

    def manifest(self, file_ids, visible_seq, protected=None, selected=None,
                 append=None, retention="rv1"):
        m = {
            "kind": "merge-on-read",
            "schema": SCHEMA,
            "files": list(self.files),
            "snapshot": {"id": "cur", "file_ids": file_ids, "visible_seq": visible_seq},
            "protected_snapshots": protected or [],
            "selected_files": selected or [],
            "retention_version": retention,
            "memory_bytes": self.memory_bytes,
            "max_open_files": self.max_open_files,
            "append_during_compaction": append or [],
        }
        validate_manifest(m)
        return m


def states(manifest, root, snapshot_id=None) -> dict:
    with read_snapshot(manifest, root, snapshot_id) as rd:
        return {r.key: (r.status, None if r.state is None else dict(r.state))
                for r in rd.results}, rd


def traces(manifest, root, snapshot_id=None) -> dict:
    with read_snapshot(manifest, root, snapshot_id) as rd:
        return {r.key: rd.trace(r) for r in rd.results}


def folding(manifest, root, snapshot_id=None) -> dict:
    """返回每键完整溯源条目（在读取句柄关闭前物化）。"""
    with read_snapshot(manifest, root, snapshot_id) as rd:
        return {r.key: list(rd.iter_provenance(r)) for r in rd.results}


def state_at(manifest, root, snapshot_id, visible_seq) -> dict:
    """在指定快照文件集上用显式 visible_seq 求每键状态。"""
    probe = json.loads(json.dumps(manifest))
    probe["snapshot"] = json.loads(json.dumps(
        next(s for s in [manifest["snapshot"]] + manifest.get("protected_snapshots", [])
             if s["id"] == snapshot_id)))
    probe["snapshot"]["visible_seq"] = visible_seq
    probe["protected_snapshots"] = []
    return states(probe, root, snapshot_id)[0]


class MergeSemanticsTests(unittest.TestCase):
    def setUp(self):
        self.d = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.d)

    def test_patch_explicit_null_differs_from_absent(self):
        b = LakeBuilder(self.d)
        # patch 只带 temperature: 显式 null；station 必须保留
        b.add("f1", [put("k", 1, "s1", 10),
                     rec("k", 2, "patch", {"temperature": None})])
        m = b.manifest(["f1"], 2)
        got, rd = states(m, self.d)
        self.assertEqual(got, {"k": (LIVE, {"station": "s1", "temperature": None})})
        # 下一个 patch 只带 station：缺席的 temperature（即便为 null）不被触碰
        b.add("f2", [rec("k", 3, "patch", {"station": "s2"})])
        m = b.manifest(["f1", "f2"], 3)
        got, _ = states(m, self.d)
        self.assertEqual(got, {"k": (LIVE, {"station": "s2", "temperature": None})})

    def test_delete_then_patch_does_not_resurrect_put_rebuilds(self):
        b = LakeBuilder(self.d)
        b.add("f1", [put("k", 1, "s1", 10), rec("k", 2, "delete"),
                     rec("k", 3, "patch", {"station": "zombie"}),
                     put("k", 4, "s2", 20)])
        m = b.manifest(["f1"], 4)
        got, _ = states(m, self.d)
        self.assertEqual(got["k"], (LIVE, {"station": "s2", "temperature": 20}))
        chain = folding(m, self.d)["k"]
        self.assertEqual([(e["op"], e["effective"]) for e in chain],
                         [("put", True), ("delete", True),
                          ("patch", False), ("put", True)])
        # 停在墓碑处：已报废设备不能被后续 patch 复活
        m3 = b.manifest(["f1"], 3)
        got, _ = states(m3, self.d)
        self.assertEqual(got["k"], (DELETED, None))

    def test_patch_without_active_put_is_evidence_only(self):
        b = LakeBuilder(self.d)
        b.add("f1", [rec("k", 1, "patch", {"station": "s1"}),
                     rec("k", 2, "delete")])
        m = b.manifest(["f1"], 2)
        got, _ = states(m, self.d)
        # 不创建部分行；无行 delete 仍留墓碑
        self.assertEqual(got["k"], (DELETED, None))
        self.assertEqual([e["op"] for e in folding(m, self.d)["k"]],
                         ["patch", "delete"])

    def test_visible_seq_fence_and_snapshot_file_isolation(self):
        b = LakeBuilder(self.d)
        b.add("f1", [put("k", 1, "s1", 10)])
        b.add("f2", [rec("k", 2, "patch", {"temperature": 11})])
        # 目录里存在但快照不引用的文件，绝不能自动纳入
        b.add("ghost", [put("k", 9, "ghost", 99)])
        m = b.manifest(["f1", "f2"], 1)
        got, _ = states(m, self.d)
        self.assertEqual(got, {"k": (LIVE, {"station": "s1", "temperature": 10})})

    def test_overlapping_files_require_full_history_not_latest_seq_file(self):
        # 三个文件各持同一键历史的一段；只挑含最新序号的文件会丢字段
        b = LakeBuilder(self.d)
        b.add("f1", [put("k", 1, "s1", 10)])
        b.add("f2", [rec("k", 2, "patch", {"temperature": 25})])
        b.add("f3", [rec("k", 3, "patch", {"station": "s3"})])
        m = b.manifest(["f1", "f2", "f3"], 3)
        got, _ = states(m, self.d)
        self.assertEqual(got, {"k": (LIVE, {"station": "s3", "temperature": 25})})

    def test_no_per_column_max_order_matters(self):
        # 后写覆盖前写，而不是各列取最大
        b = LakeBuilder(self.d)
        b.add("f1", [put("k", 1, "s1", 10),
                     rec("k", 2, "patch", {"temperature": 5}),
                     rec("k", 3, "patch", {"temperature": 7})])
        m = b.manifest(["f1"], 3)
        got, _ = states(m, self.d)
        self.assertEqual(got["k"][1]["temperature"], 7)

    def test_same_order_same_bytes_replays_other_bytes_conflict(self):
        r1 = put("k", 1, "s1", 10)
        b = LakeBuilder(self.d)
        b.add("f1", [r1])
        b.add("f2", [json.loads(json.dumps(r1))])  # 同序同字节
        m = b.manifest(["f1", "f2"], 1)
        got, _ = states(m, self.d)
        self.assertEqual(got["k"][0], LIVE)
        entry = folding(m, self.d)["k"][0]
        self.assertEqual(sorted(entry["sources"]), ["f1", "f2"])
        # 同序不同字节：严格总序冲突
        b2 = LakeBuilder(Path(self.d) / "c2")
        (Path(self.d) / "c2").mkdir()
        b2.add("g1", [put("k", 1, "s1", 10)])
        b2.add("g2", [put("k", 1, "s2", 10)])
        with self.assertRaises(MergeConflict):
            states(b2.manifest(["g1", "g2"], 1), Path(self.d) / "c2")

    def test_ordinal_total_order_within_commit(self):
        b = LakeBuilder(self.d)
        b.add("f1", [put("k", 1, "s1", 1, ordinal=2),
                     rec("k", 1, "patch", {"temperature": 2}, ordinal=5)])
        m = b.manifest(["f1"], 1)
        got, _ = states(m, self.d)
        self.assertEqual(got["k"][1]["temperature"], 2)

    def test_unsorted_file_rejected(self):
        b = LakeBuilder(self.d)
        b.add("f1", [put("b", 1, "s", 1), put("a", 1, "s", 1)])
        m = b.manifest(["f1"], 1)
        with self.assertRaises(TableError):
            states(m, self.d)


class ResourceBoundTests(unittest.TestCase):
    def setUp(self):
        self.d = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.d)

    def test_multi_pass_merge_respects_open_file_budget(self):
        # 6 个文件、预算 3：fan_in=2，必须多趟外部归并
        b = LakeBuilder(self.d, max_open_files=3)
        for i in range(6):
            b.add(f"f{i}", [put(f"k{i}", 1, f"s{i}", i)])
        m = b.manifest([f"f{i}" for i in range(6)], 1)
        got, rd = states(m, self.d)
        self.assertEqual(len(got), 6)
        self.assertLessEqual(rd.budget.peak, 3)
        self.assertEqual(got["k5"], (LIVE, {"station": "s5", "temperature": 5}))

    def test_oversized_key_group_spills_provenance_but_not_whole_list(self):
        b = LakeBuilder(self.d, memory_bytes=120, max_open_files=3)
        rows = [put("k", 1, "s0", 0)]
        for seq in range(2, 40):
            rows.append(rec("k", seq, "patch", {"temperature": seq}))
        b.add("f1", rows)
        m = b.manifest(["f1"], 39)
        with read_snapshot(m, self.d) as rd:
            self.assertEqual(len(rd.results), 1)
            result = rd.results[0]
            self.assertTrue(any("spill" in c for c in result.provenance_chunks))
            chain = list(rd.iter_provenance(result))  # 流式回放，不整组载入
            self.assertEqual(len(chain), 39)
            self.assertEqual(result.state, {"station": "s0", "temperature": 39})

    def test_budget_below_three_rejected(self):
        b = LakeBuilder(self.d, max_open_files=1)
        b.add("f1", [put("k", 1, "s", 1)])
        with self.assertRaises((TableError, ValueError)):
            b.manifest(["f1"], 1)

    def test_handle_budget_hard_limit(self):
        budget = HandleBudget(3)
        paths = [self.d / f"h{i}" for i in range(3)]
        for p in paths:
            p.write_bytes(b"x")
        with budget.opening(paths[0]), budget.opening(paths[1]), budget.opening(paths[2]):
            with self.assertRaises(TableError):
                with budget.opening(paths[0]):
                    pass


class RecordValidationTests(unittest.TestCase):
    def test_record_shapes(self):
        ok = put("k", 1, "s1", 10)
        self.assertEqual(validate_record(ok, SCHEMA)["value"], ok["value"])
        validate_record(rec("k", 1, "patch", {"temperature": None}), SCHEMA)
        validate_record(rec("k", 1, "delete"), SCHEMA)
        bad = [
            {"key": "k", "seq": 0, "ordinal": 0, "op": "put",
             "value": {"station": "s1"}},                       # put 缺字段
            {"key": "k", "seq": 0, "ordinal": 0, "op": "put",
             "value": {"station": "s1", "temperature": 1, "x": 2}},  # put 额外字段
            {"key": "k", "seq": 0, "ordinal": 0, "op": "patch",
             "value": {"unknown": 1}},                          # patch 未知列
            {"key": "k", "seq": 0, "ordinal": 0, "op": "patch",
             "value": {"station": None}},                       # station 不可 null
            {"key": "k", "seq": 0, "ordinal": 0, "op": "patch",
             "value": {"temperature": 1.5}},                    # 温度必须整数
            {"key": "k", "seq": -1, "ordinal": 0, "op": "delete"},
            {"key": "k", "seq": 0, "ordinal": 0, "op": "delete", "value": {}},
            {"key": "", "seq": 0, "ordinal": 0, "op": "delete"},
            {"key": "k", "seq": True, "ordinal": 0, "op": "delete"},
        ]
        for x in bad:
            with self.assertRaises(TableError):
                validate_record(x, SCHEMA)

    def test_canonical_bytes(self):
        r1 = put("k", 1, "s1", 10)
        r2 = {"op": "put", "ordinal": 0, "seq": 1, "key": "k",
              "value": {"temperature": 10, "station": "s1"}}
        self.assertEqual(canonical(r1), canonical(r2))
        r3 = dict(r2, value={"station": "s1", "temperature": 11})
        self.assertNotEqual(canonical(r1), canonical(r3))


class CompactionTests(unittest.TestCase):
    def setUp(self):
        self.d = Path(tempfile.mkdtemp())
        self.out = self.d / "out"
        self.addCleanup(shutil.rmtree, self.d)

    def _copy_assets(self, manifest, src: Path, dst: Path):
        dst.mkdir(parents=True, exist_ok=True)
        for f in manifest["files"]:
            shutil.copy2(src / f["asset"]["path"], dst / f["asset"]["path"])

    def _equivalent(self, before, src, after, dst):
        """全部受保护快照逐轨迹全等；当前快照每键状态在原围栏全等。"""
        for s in before["protected_snapshots"]:
            self.assertEqual(traces(before, src, s["id"]),
                             traces(after, dst, s["id"]))
        old_state, _ = states(before, src)
        new_state, _ = states(after, dst)
        self.assertEqual(old_state, new_state)

    def test_compaction_keeps_tombstone_when_unselected_file_hides_old_put(self):
        b = LakeBuilder(self.d)
        b.add("base", [put("a", 1, "s1", 10), put("b", 1, "s2", 20, ordinal=1)])
        b.add("delta", [rec("a", 2, "patch", {"temperature": None}),
                        rec("a", 3, "delete"),
                        rec("a", 4, "patch", {"station": "s3"}),
                        rec("b", 2, "patch", {"temperature": 25}, ordinal=1)])
        b.add("new", [put("a", 5, "s4", 30)])
        m = b.manifest(
            ["base", "delta", "new"], 5,
            protected=[{"id": "s2", "file_ids": ["base", "delta"], "visible_seq": 2},
                       {"id": "s4", "file_ids": ["base", "delta"], "visible_seq": 4}],
            selected=["delta"])
        self._copy_assets(m, self.d, self.out)
        report = execute_compaction(m, self.out, self.out)
        after = json.loads((self.out / "manifest-after.json").read_text())
        # 未选 base 藏有 a 的更早 put（seq 1 <= 墓碑 seq 3），墓碑不可消去
        proof = report["coverage_proof"]["tombstone_proofs"][0]
        self.assertFalse(proof["eliminable"])
        self.assertEqual(proof["unselected_min_seq"], 1)
        self.assertEqual(report["compacted_rows"]["dropped"], [])
        self.assertEqual(report["file_mapping"]["reclaimed"], [])
        self._equivalent(m, self.d, after, self.out)
        # 受保护快照仍读原始文件，文件集原样保留
        self.assertEqual(after["protected_snapshots"], m["protected_snapshots"])
        self.assertIn("delta", [f["id"] for f in after["files"]])

    def test_tombstone_eliminable_only_with_full_coverage_proof(self):
        # 未选文件不含该键 → 覆盖证明成立，墓碑与窗口内未生效 patch 一起消去
        b = LakeBuilder(self.d)
        b.add("f1", [put("x", 1, "s1", 1), rec("x", 3, "delete"),
                     rec("x", 4, "patch", {"station": "zombie"}),
                     put("x", 5, "s2", 2)])
        b.add("f2", [put("y", 1, "s3", 3)])
        m = b.manifest(["f1", "f2"], 5,
                       protected=[{"id": "s0", "file_ids": ["f2"], "visible_seq": 1}],
                       selected=["f1"])
        self._copy_assets(m, self.d, self.out)
        report = execute_compaction(m, self.out, self.out)
        proof = report["coverage_proof"]["tombstone_proofs"][0]
        self.assertTrue(proof["eliminable"])
        self.assertIsNone(proof["unselected_min_seq"])
        dropped = {(d["key"], d["seq"], d["reason"]) for d in
                   report["compacted_rows"]["dropped"]}
        self.assertEqual(dropped, {("x", 3, "eliminated-tombstone"),
                                   ("x", 4, "ineffective-before-next-put")})
        compacted = [json.loads(line) for line in
                     (self.out / "compacted.jsonl").read_text().splitlines()]
        self.assertEqual([r["seq"] for r in compacted], [1, 5])
        after = json.loads((self.out / "manifest-after.json").read_text())
        self._equivalent(m, self.d, after, self.out)

    def test_unselected_witness_after_tombstone_does_not_block_elimination(self):
        b = LakeBuilder(self.d)
        b.add("f1", [put("x", 1, "s1", 1), rec("x", 3, "delete"),
                     put("x", 5, "s2", 2)])
        # 未选文件里同键记录 seq=10 晚于墓碑 seq 3，不构成“更早版本”见证
        b.add("f2", [rec("x", 10, "patch", {"temperature": 9})])
        m = b.manifest(["f1", "f2"], 10, selected=["f1"])
        self._copy_assets(m, self.d, self.out)
        report = execute_compaction(m, self.out, self.out)
        proof = report["coverage_proof"]["tombstone_proofs"][0]
        self.assertTrue(proof["eliminable"])
        self.assertEqual(proof["unselected_min_seq"], 10)

    def test_delete_then_patch_across_segments_survives_compaction(self):
        # 墓碑与随后的 patch 分处不同文件；无后续完整 put，墓碑不可消去
        b = LakeBuilder(self.d)
        b.add("f1", [put("a", 1, "s1", 10), rec("a", 2, "delete")])
        b.add("f2", [rec("a", 3, "patch", {"station": "zombie"})])
        m = b.manifest(["f1", "f2"], 3, selected=["f1", "f2"])
        self._copy_assets(m, self.d, self.out)
        report = execute_compaction(m, self.out, self.out)
        after = json.loads((self.out / "manifest-after.json").read_text())
        self.assertEqual(states(m, self.d)[0], states(after, self.out)[0])
        compacted = [json.loads(line) for line in
                     (self.out / "compacted.jsonl").read_text().splitlines()]
        self.assertEqual([(r["seq"], r["op"]) for r in compacted],
                         [(1, "put"), (2, "delete"), (3, "patch")])
        proof = report["coverage_proof"]["tombstone_proofs"][0]
        self.assertFalse(proof["eliminable"])
        self.assertIsNone(proof["rebuild_put_seq"])
        self.assertEqual(report["compacted_rows"]["dropped"], [])

    def test_tombstone_without_rebuild_put_is_never_eliminated(self):
        # 即使覆盖证明成立，没有后续完整 put 的墓碑也必须保留
        b = LakeBuilder(self.d)
        b.add("f1", [put("a", 1, "s1", 10), rec("a", 2, "delete"),
                     rec("a", 3, "patch", {"station": "zombie"})])
        b.add("f2", [put("b", 1, "s2", 20)])
        m = b.manifest(["f1", "f2"], 3, selected=["f1"])
        self._copy_assets(m, self.d, self.out)
        report = execute_compaction(m, self.out, self.out)
        proof = report["coverage_proof"]["tombstone_proofs"][0]
        self.assertFalse(proof["eliminable"])
        self.assertIsNone(proof["unselected_min_seq"])  # 覆盖证明本身成立
        self.assertIsNone(proof["rebuild_put_seq"])
        after = json.loads((self.out / "manifest-after.json").read_text())
        self.assertEqual(states(m, self.d)[0], states(after, self.out)[0])
        self.assertEqual(states(after, self.out)[0]["a"], (DELETED, None))

    def test_same_key_across_segments_merges(self):
        b = LakeBuilder(self.d)
        b.add("f1", [put("a", 1, "s1", 10), put("b", 1, "sb", 1, ordinal=1)])
        b.add("f2", [rec("a", 2, "patch", {"temperature": 11}),
                     rec("b", 2, "patch", {"station": "sb2"}, ordinal=1)])
        m = b.manifest(["f1", "f2"], 2, selected=["f1", "f2"])
        self._copy_assets(m, self.d, self.out)
        execute_compaction(m, self.out, self.out)
        after = json.loads((self.out / "manifest-after.json").read_text())
        got, _ = states(after, self.out)
        self.assertEqual(got, {
            "a": (LIVE, {"station": "s1", "temperature": 11}),
            "b": (LIVE, {"station": "sb2", "temperature": 1}),
        })

    def test_sealed_append_is_retained_and_protected_snapshots_untouched(self):
        b = LakeBuilder(self.d)
        b.add("base", [put("a", 1, "s1", 10)])
        b.add("delta", [rec("a", 2, "patch", {"temperature": 11})])
        append = [rec("a", 6, "patch", {"station": "s9"})]
        m = b.manifest(
            ["base", "delta"], 5,
            protected=[{"id": "s2", "file_ids": ["base", "delta"], "visible_seq": 2}],
            selected=["delta"], append=append)
        self._copy_assets(m, self.d, self.out)
        report = execute_compaction(m, self.out, self.out)
        after = json.loads((self.out / "manifest-after.json").read_text())
        self.assertEqual(report["plan"]["append_seqs"], [6])
        self.assertEqual(report["file_mapping"]["sealed_append"], "sealed-append")
        self.assertEqual(after["snapshot"]["visible_seq"], 6)
        self.assertIn("sealed-append", after["snapshot"]["file_ids"])
        self.assertEqual(after["protected_snapshots"], m["protected_snapshots"])
        got, _ = states(after, self.out)
        self.assertEqual(got["a"], (LIVE, {"station": "s9", "temperature": 11}))
        # 受保护快照看不到 seq 6
        old, _ = states(m, self.d, "s2")
        new, _ = states(after, self.out, "s2")
        self.assertEqual(old, new)

    def test_commit_rejects_changed_identity_retention_and_protected_set(self):
        b = LakeBuilder(self.d)
        b.add("f1", [put("a", 1, "s1", 10), rec("a", 2, "delete")])
        b.add("f2", [put("b", 1, "s2", 20)])
        m = b.manifest(["f1", "f2"], 2,
                       protected=[{"id": "p1", "file_ids": ["f2"], "visible_seq": 1}],
                       selected=["f1"])
        self._copy_assets(m, self.d, self.out)

        def change_identity(seen, out_dir):
            for f in seen["files"]:
                if f["id"] == "f1":
                    f["asset"]["sha256"] = "0" * 64
            return seen

        def change_retention(seen, out_dir):
            seen["retention_version"] = "rv2"
            return seen

        def change_protected(seen, out_dir):
            seen["protected_snapshots"].append(
                {"id": "p2", "file_ids": ["f2"], "visible_seq": 2})
            return seen

        for hook in (change_identity, change_retention, change_protected):
            with self.subTest(hook=hook.__name__):
                with self.assertRaises(CommitConflict):
                    execute_compaction(m, self.out, self.out, before_commit=hook)

    def test_commit_keeps_concurrent_new_file_appended_during_compute(self):
        b = LakeBuilder(self.d)
        b.add("f1", [put("a", 1, "s1", 10)])
        m = b.manifest(["f1"], 1, selected=["f1"])
        self._copy_assets(m, self.d, self.out)
        extra_rows = [rec("c", 7, "patch", {"station": "live-concurrent"})]

        def hook(seen, out_dir):
            asset = write_jsonl(out_dir / "concurrent.jsonl", extra_rows)
            seen["files"].append({"id": "concurrent", "asset": asset})
            seen["snapshot"]["file_ids"].append("concurrent")
            seen["snapshot"]["visible_seq"] = 7  # 并发提交同时推进围栏
            return seen

        report = execute_compaction(m, self.out, self.out, before_commit=hook)
        after = json.loads((self.out / "manifest-after.json").read_text())
        self.assertIn("concurrent", [f["id"] for f in after["files"]])
        self.assertIn("concurrent", after["snapshot"]["file_ids"])
        got, _ = states(after, self.out)
        self.assertEqual(got["c"], (ABSENT, None))  # 无活跃 put，patch 只是证据
        self.assertEqual(report["equivalence_check"]["result"], "identical")

    def test_plan_binding_fingerprint_and_empty_selection(self):
        b = LakeBuilder(self.d)
        b.add("f1", [put("a", 1, "s1", 10)])
        m = b.manifest(["f1"], 1, selected=["f1"])
        plan = plan_compaction(m)
        self.assertEqual(len(plan.binding_fingerprint), 64)
        self.assertEqual(plan.visible_seq_fence, 1)
        empty = b.manifest(["f1"], 1, selected=[])
        with self.assertRaises(TableError):
            plan_compaction(empty)

    def test_provided_fixtures_compact_equivalently(self):
        fixtures = Path(__file__).resolve().parents[1] / "data"
        for name in ("normal.json", "delete-then-patch.json", "concurrent-append.json"):
            with self.subTest(fixture=name):
                manifest = json.loads((fixtures / name).read_text())
                work = Path(tempfile.mkdtemp())
                self.addCleanup(shutil.rmtree, work)
                for f in manifest["files"]:
                    shutil.copy2(fixtures / f["asset"]["path"],
                                 work / f["asset"]["path"])
                execute_compaction(manifest, work, work)
                after = json.loads((work / "manifest-after.json").read_text())
                for s in manifest["protected_snapshots"]:
                    self.assertEqual(traces(manifest, fixtures, s["id"]),
                                     traces(after, work, s["id"]))
                old_fence = manifest["snapshot"]["visible_seq"]
                if manifest.get("append_during_compaction"):
                    # 追加封存让当前快照围栏前移：在原围栏上状态必须全等
                    self.assertEqual(
                        states(manifest, fixtures)[0],
                        state_at(after, work, after["snapshot"]["id"], old_fence))
                    added = {r["seq"] for r in manifest["append_during_compaction"]}
                    affected = {r["key"] for r in manifest["append_during_compaction"]}
                    for key in affected:
                        tail = [t[0] for t in traces(after, work)[key]
                                if t[0] > old_fence]
                        self.assertTrue(tail and all(s in added for s in tail))
                else:
                    self.assertEqual(states(manifest, fixtures)[0],
                                     states(after, work)[0])

    def test_provided_invalid_fixture_rejected(self):
        fixtures = Path(__file__).resolve().parents[1] / "data"
        manifest = json.loads((fixtures / "invalid-config.json").read_text())
        with self.assertRaises((TableError, ValueError)):
            validate_manifest(manifest)


if __name__ == "__main__":
    unittest.main()
