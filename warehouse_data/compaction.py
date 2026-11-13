"""在线压实：合并小文件，且不改变任何已发布（受保护）读版本。

安全约束（见 docs/data-contract.md）：

* 计划绑定所选文件身份（路径/字节数/sha256）、受保护快照集合、retention_version
  与版本围栏；计算阶段允许其他写者追加文件；
* 提交时只替换仍存在且身份相同的选中文件，并发追加全部保留；受保护快照集合或
  保留策略变化则放弃并重建计划；本次不物理回收任何对象，旧文件继续可读；
* 产物保留基准状态与后续增量（默认逐事件保留，可在覆盖证明成立时消去墓碑）；
* 墓碑消去必须证明：该键的更早版本不可能留在任何未选文件中，且该键自墓碑至下
  一个完整 put 之间没有需要保留的未生效记录。不能用“选中文件里没有旧行”代替
  整个快照范围内的覆盖证明。
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path

from .io import asset, integer
from .table import (
    ABSENT,
    DELETE,
    LIVE,
    PATCH,
    PUT,
    HandleBudget,
    TableError,
    canonical,
    file_runs,
    fold,
    merged_events,
    read_snapshot,
    resolve_snapshot,
    validate_manifest,
    validate_record,
)

class CommitConflict(TableError):
    """提交对账失败：文件身份、受保护快照或保留策略在计算期间发生变化。"""


@dataclass
class Plan:
    retention_version: str
    selected: list[dict]                 # 选中文件的完整描述（含资产身份）
    protected: list[dict]                # 受保护快照集合（深拷贝）
    current_snapshot: dict               # 当前快照（深拷贝）
    visible_seq_fence: int               # 计划可见版本围栏
    append_seqs: list[int]               # 调度点可信追加的 seq 集合
    binding_fingerprint: str


def plan_compaction(manifest: dict) -> Plan:
    """根据清单建立压实计划；此时不写任何文件。"""
    validate_manifest(manifest)
    by_id = {f["id"]: f for f in manifest["files"]}
    selected_ids = list(manifest.get("selected_files", []))
    if not selected_ids:
        raise TableError("没有选中任何文件，压实计划为空")
    selected = [json.loads(json.dumps(by_id[fid])) for fid in selected_ids]
    protected = json.loads(json.dumps(manifest.get("protected_snapshots", [])))
    current = json.loads(json.dumps(manifest["snapshot"]))
    append_seqs = [integer(r["seq"], 0) for r in manifest.get("append_during_compaction", [])]
    binding = {
        "retention_version": manifest["retention_version"],
        "selected": selected,
        "protected": protected,
        "snapshot": current,
        "append_seqs": sorted(append_seqs),
    }
    raw = json.dumps(binding, sort_keys=True, ensure_ascii=False).encode("utf-8")
    return Plan(
        retention_version=manifest["retention_version"],
        selected=selected,
        protected=protected,
        current_snapshot=current,
        visible_seq_fence=current["visible_seq"],
        append_seqs=sorted(append_seqs),
        binding_fingerprint=hashlib.sha256(raw).hexdigest(),
    )


def _write_jsonl(path: Path, records: list[dict]) -> None:
    lines = [json.dumps(r, ensure_ascii=False, separators=(",", ":")) for r in records]
    path.write_bytes(("".join(line + "\n" for line in lines)).encode("utf-8"))


def _seal_appends(manifest: dict, out_dir: Path, plan: Plan) -> tuple[list[dict], list[dict]]:
    """把调度点中的可信新增事件封存为按总序排序的新文件。"""
    rows = [validate_record(r, manifest["schema"])
            for r in manifest.get("append_during_compaction", [])]
    if not rows:
        return [], []
    rows.sort(key=lambda r: (r["key"], r["seq"], r["ordinal"]))
    last = None
    for r in rows:
        triple = (r["key"], r["seq"], r["ordinal"])
        if last is not None and triple <= last:
            raise TableError("追加事件内部存在重复总序")
        last = triple
    path = out_dir / "append-sealed.jsonl"
    _write_jsonl(path, rows)
    raw = path.read_bytes()
    ref = {
        "id": "sealed-append",
        "asset": {
            "path": path.name,
            "bytes": len(raw),
            "sha256": hashlib.sha256(raw).hexdigest(),
            "rows": len(rows),
        },
    }
    return [ref], rows


def _key_summary(manifest: dict, plan: Plan, root: Path, budget: HandleBudget,
                 tmpdir: Path) -> dict:
    """第一遍归并选中文件：得到每键摘要（墓碑 seq、各来源 seq 范围等）。"""
    all_runs = file_runs(manifest, root)
    runs = [all_runs[f["id"]] for f in plan.selected]
    summary: dict[str, dict] = {}
    for ev in merged_events(runs, budget, manifest["schema"],
                            visible_seq=None, tmpdir=tmpdir):
        item = summary.setdefault(ev.key, {
            "key": ev.key, "tombstones": [], "events": 0, "max_seq": -1,
            "puts": [], "status_after_selected": ABSENT, "live_state": None,
        })
        item["events"] += 1
        item["max_seq"] = max(item["max_seq"], ev.seq)
        rec = ev.rec
        if rec["op"] == PUT:
            item["puts"].append(ev.seq)
            item["status_after_selected"] = LIVE
            item["live_state"] = dict(rec["value"])
        elif rec["op"] == DELETE:
            item["tombstones"].append(ev.seq)
            item["status_after_selected"] = "deleted"
            item["live_state"] = None
        elif rec["op"] == PATCH and item["status_after_selected"] == LIVE:
            for column, value in rec["value"].items():
                item["live_state"][column] = value
    return summary


def coverage_proof(manifest: dict, plan: Plan, root: Path, budget: HandleBudget,
                   tmpdir: Path) -> dict:
    """扫描全部未选文件，为每个墓碑构造“更早版本是否可能留在未选文件”的证明。

    证明基于整个快照可见的文件全集，而不是选中文件内部是否缺旧行：
    未选文件中只要出现同键且 seq 不高于墓碑 seq 的记录，该墓碑就不可消去。
    """
    all_runs = file_runs(manifest, root)
    selected_ids = {f["id"] for f in plan.selected}
    unselected = [run for fid, run in all_runs.items() if fid not in selected_ids]
    # 未选文件每键最小 seq，流式扫描；只保留整数，内存与键数同阶且很小。
    min_seq_by_key: dict[str, int] = {}
    for ev in merged_events(unselected, budget, manifest["schema"],
                            visible_seq=None, tmpdir=tmpdir):
        prev = min_seq_by_key.get(ev.key)
        min_seq_by_key[ev.key] = ev.seq if prev is None else min(prev, ev.seq)

    summary = _key_summary(manifest, plan, root, budget, tmpdir)
    proofs = []
    for key in sorted(summary):
        item = summary[key]
        for tomb_seq in item["tombstones"]:
            witness = min_seq_by_key.get(key)
            coverage_ok = witness is None or witness > tomb_seq
            # 墓碑之后必须有完整 put 在选中范围内重建行；否则消去墓碑会让
            # 已删除的键凭空复活，后续 patch 也不能替代这个重建。
            rebuild_seq = next((s for s in item["puts"] if s > tomb_seq), None)
            eliminable = coverage_ok and rebuild_seq is not None
            if not coverage_ok:
                basis = ("未选文件存在 seq <= 墓碑 seq 的同键记录，旧版本可能仍被受保护"
                         "读取折叠到，墓碑必须保留")
            elif rebuild_seq is None:
                basis = ("覆盖证明成立但墓碑之后没有完整 put 重建，消去墓碑会让已删除"
                         "的键复活，墓碑必须保留")
            else:
                basis = ("未选文件无该键记录，或未选文件该键最小 seq 大于墓碑 seq，"
                          "且墓碑后存在完整 put 重建，可证明更早版本不可能留在任何"
                          "未选文件，墓碑及窗口内未生效记录可消去")
            proofs.append({
                "key": key,
                "tombstone_seq": tomb_seq,
                "unselected_min_seq": witness,
                "rebuild_put_seq": rebuild_seq,
                "eliminable": eliminable,
                "basis": basis,
            })
    return {
        "unselected_files": sorted(fid for fid in all_runs if fid not in selected_ids),
        "unselected_min_seq_by_key": dict(sorted(min_seq_by_key.items())),
        "tombstone_proofs": proofs,
        "key_summary": summary,
    }


def _elimination_ranges(proof: dict) -> dict:
    """每个可消去墓碑的 seq（后续写入时据此跳过墓碑及之后到下个 put 前的记录）。"""
    ranges: dict[str, list[int]] = {}
    for p in proof["tombstone_proofs"]:
        if p["eliminable"]:
            ranges.setdefault(p["key"], []).append(p["tombstone_seq"])
    return ranges


def _compact_selected(manifest: dict, plan: Plan, root: Path, out_dir: Path,
                      budget: HandleBudget, tmpdir: Path, proof: dict) -> dict:
    """第二遍归并选中文件并写出压实产物，返回新文件描述。"""
    all_runs = file_runs(manifest, root)
    runs = [all_runs[f["id"]] for f in plan.selected]
    eliminable = _elimination_ranges(proof)
    # 记录每个键当前是否处于“墓碑已消去、等待完整 put 重建”的窗口。
    dead_window: dict[str, int] = {}
    out_records: list[dict] = []
    skipped: list[dict] = []
    for ev in merged_events(runs, budget, manifest["schema"],
                            visible_seq=None, tmpdir=tmpdir):
        rec = ev.rec
        key = ev.key
        if rec["op"] == PUT:
            dead_window.pop(key, None)
        elif key in dead_window:
            if rec["seq"] < dead_window[key]:
                raise TableError("归并序列异常：窗口内记录早于墓碑")
            if rec["op"] == DELETE:
                # 窗口内又遇墓碑：若也可消去则延续窗口，否则窗口关闭并保留该墓碑
                if rec["seq"] in eliminable.get(key, []):
                    dead_window[key] = rec["seq"]
                    skipped.append({"key": key, "seq": rec["seq"],
                                    "ordinal": rec["ordinal"], "op": DELETE,
                                    "reason": "eliminated-tombstone"})
                    continue
                dead_window.pop(key, None)
            else:  # 墓碑后的 patch 不能凭空复活，随墓碑一起消去
                skipped.append({"key": key, "seq": rec["seq"],
                                "ordinal": rec["ordinal"], "op": rec["op"],
                                "reason": "ineffective-before-next-put"})
                continue
        if rec["op"] == DELETE and rec["seq"] in eliminable.get(key, []):
            dead_window[key] = rec["seq"]
            skipped.append({"key": key, "seq": rec["seq"],
                            "ordinal": rec["ordinal"], "op": DELETE,
                            "reason": "eliminated-tombstone"})
            continue
        out_records.append(rec)

    path = out_dir / "compacted.jsonl"
    _write_jsonl(path, out_records)
    raw = path.read_bytes()
    return {
        "id": "compacted",
        "asset": {
            "path": path.name,
            "bytes": len(raw),
            "sha256": hashlib.sha256(raw).hexdigest(),
            "rows": len(out_records),
        },
        "kept_rows": len(out_records),
        "skipped": skipped,
    }


def _semantics_at(manifest: dict, root: Path, snapshot_id: str,
                  visible_seq: int) -> dict:
    """在指定快照文件集上用显式 visible_seq 求语义结果（围栏不改变文件集）。"""
    snapshot = json.loads(json.dumps(resolve_snapshot(manifest, snapshot_id)))
    snapshot["visible_seq"] = visible_seq
    probe = json.loads(json.dumps(manifest))
    probe["snapshot"] = snapshot
    probe["protected_snapshots"] = []
    with read_snapshot(probe, root, snapshot["id"]) as rd:
        return {r.key: {"status": r.status, "state": r.state,
                        "trace": rd.trace(r)} for r in rd.results}


def _snapshot_semantics(manifest: dict, root: Path) -> list[dict]:
    """对当前快照和全部受保护快照求语义结果（状态 + 折叠轨迹）。"""
    ids = [manifest["snapshot"]["id"]] + [s["id"] for s in manifest.get("protected_snapshots", [])]
    out = []
    for sid in ids:
        with read_snapshot(manifest, root, sid) as rd:
            out.append({
                "snapshot_id": sid,
                "visible_seq": rd.snapshot["visible_seq"],
                "keys": {r.key: {"status": r.status, "state": r.state,
                                 "trace": rd.trace(r)} for r in rd.results},
            })
    return out


def _assert_equivalent(before_manifest: dict, before_root: Path,
                       after_manifest: dict, after_root: Path,
                       before_semantics: list[dict], proof: dict) -> None:
    """压实前后读取等价校验。

    受保护快照：逐键比较 status/state/完整折叠轨迹，不能只比最终行数；其文件集
    原样保留，任何轨迹差异都意味着已发布读版本被改变。
    当前快照：受保护快照之外的工作版本。在原围栏 visible_seq 上比较最终
    status/state；折叠轨迹只允许在覆盖证明成立的墓碑消去窗口内变化，且每个被
    消去的墓碑必须在证明中标记 eliminable。围栏前移只能来自计划内追加事件。
    """
    eliminable = {(p["key"], p["tombstone_seq"])
                  for p in proof["tombstone_proofs"] if p["eliminable"]}
    after_by_id = {s["snapshot_id"]: s for s in
                   _snapshot_semantics(after_manifest, after_root)}
    for old in before_semantics:
        new = after_by_id.get(old["snapshot_id"])
        if new is None:
            raise CommitConflict(f"压实后快照 {old['snapshot_id']} 消失")
        if old["snapshot_id"] == before_manifest["snapshot"]["id"]:
            new_at_fence = _semantics_at(
                after_manifest, after_root, old["snapshot_id"], old["visible_seq"])
            # 最终语义（每键 status/state）必须完全一致。
            old_sem = {k: (v["status"], json.dumps(v["state"], sort_keys=True))
                       for k, v in old["keys"].items()}
            new_sem = {k: (v["status"], json.dumps(v["state"], sort_keys=True))
                       for k, v in new_at_fence.items()}
            if old_sem != new_sem:
                diffs = sorted(set(old_sem) | set(new_sem))
                raise CommitConflict(
                    f"当前快照在原围栏 {old['visible_seq']} 的每键状态压实前后不一致: {diffs}")
            # 轨迹差异只能落在有覆盖证明背书的墓碑消去窗口内：被移除的连续片段
            # 必须以可消去墓碑开始，且其后不包含完整 put（put 重建不可能被跳过）。
            for key, old_value in old["keys"].items():
                new_value = new_at_fence.get(key)
                if new_value is None or old_value["trace"] == new_value["trace"]:
                    continue
                removed = [t for t in old_value["trace"] if t not in new_value["trace"]]
                window_open = False
                for seq, _ordinal, op, _effective in removed:
                    if op == DELETE and (key, seq) in eliminable:
                        window_open = True
                        continue
                    if window_open and op != PUT:
                        continue  # 墓碑到下一个完整 put 之间的未生效记录随墓碑消去
                    raise CommitConflict(
                        f"键 {key} 的折叠轨迹在无覆盖证明的情况下被改变: {removed}")
            # 前移部分：新围栏下的变化只能归因于计划内可信追加事件。
            added_seqs = {r["seq"] for r in
                          before_manifest.get("append_during_compaction", [])}
            if added_seqs:
                for key, value in new["keys"].items():
                    if key in old["keys"] and old["keys"][key] != value:
                        tail = [t[0] for t in value["trace"]
                                if t[0] > old["visible_seq"]]
                        if not tail or any(seq not in added_seqs for seq in tail):
                            raise CommitConflict(
                                f"键 {key} 在新围栏下的变化无法归因于计划内追加事件")
        else:
            if new["visible_seq"] != old["visible_seq"]:
                raise CommitConflict(f"受保护快照 {old['snapshot_id']} 围栏被修改")
            if old["keys"] != new["keys"]:
                diffs = sorted(set(old["keys"]) | set(new["keys"]))
                raise CommitConflict(
                    f"受保护读取 {old['snapshot_id']} 在压实前后不一致，差异键: {diffs}")


def _reconcile(plan: Plan, current_manifest: dict) -> None:
    """提交围栏：重新核对计划绑定的文件身份、受保护快照与保留策略。"""
    if current_manifest.get("retention_version") != plan.retention_version:
        raise CommitConflict("保留策略（retention_version）已变化，必须重建计划")
    current_protected = json.loads(json.dumps(current_manifest.get("protected_snapshots", [])))
    if canonical({"x": current_protected}) != canonical({"x": plan.protected}):
        raise CommitConflict("受保护快照集合已变化，必须重建计划")
    if canonical({"x": current_manifest["snapshot"]}) != canonical(
            {"x": plan.current_snapshot}):
        # 当前快照本身可以因并发追加而更新（其 file_ids/visible_seq 变化），
        # 但围栏 visible_seq 不得倒退。
        if current_manifest["snapshot"]["visible_seq"] < plan.visible_seq_fence:
            raise CommitConflict("当前快照 visible_seq 倒退，版本围栏被破坏")
    by_id = {f["id"]: f for f in current_manifest["files"]}
    for chosen in plan.selected:
        now = by_id.get(chosen["id"])
        if now is None:
            raise CommitConflict(f"选中文件 {chosen['id']} 已消失，提交被拒绝")
        if canonical(now) != canonical(chosen):
            raise CommitConflict(f"选中文件 {chosen['id']} 身份变化，提交被拒绝")


def execute_compaction(manifest: dict, root: Path, out_dir: Path, *,
                       before_commit=None) -> dict:
    """执行一次完整压实，返回报告；新清单与产物写入 ``out_dir``。

    ``before_commit(current_manifest, root) -> current_manifest`` 是可控调度点，
    用于模拟计算期间其他写者追加文件；返回的清单即提交时看到的仓库状态。
    本次不物理回收旧对象：所有原文件身份原样保留，受保护快照继续读原文件。
    """
    plan = plan_compaction(manifest)
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    reserved = {"compacted.jsonl", "append-sealed.jsonl",
                "manifest-after.json", "compaction-report.json"}
    occupied = {f["asset"]["path"] for f in manifest["files"]}
    clash = reserved & occupied
    if clash:
        raise CommitConflict(f"输出文件名与现有资产冲突: {sorted(clash)}")
    work = Path(tempfile.mkdtemp(prefix="compact-", dir=out_dir))
    budget = HandleBudget(manifest["max_open_files"])
    try:
        # 调度点中的可信新增事件：封存为新文件，计算与提交都必须保留。
        sealed_refs, sealed_rows = _seal_appends(manifest, out_dir, plan)

        proof = coverage_proof(manifest, plan, root, budget, work)
        compacted = _compact_selected(manifest, plan, root, out_dir, budget, work, proof)

        before_semantics = _snapshot_semantics(manifest, root)

        # 组装新清单：原文件全部保留（不回收），追加与压实产物并入文件表。
        new_manifest = json.loads(json.dumps(manifest))
        new_manifest["append_during_compaction"] = []
        existing_ids = {f["id"] for f in new_manifest["files"]}
        for ref in sealed_refs:
            if ref["id"] in existing_ids:
                raise CommitConflict(f"新文件 id {ref['id']} 与现有文件冲突")
            new_manifest["files"].append(ref)
        if "compacted" in existing_ids:
            raise CommitConflict("compacted 文件 id 已存在")
        new_manifest["files"].append({k: v for k, v in compacted.items()
                                      if k in ("id", "asset")})

        selected_ids = {f["id"] for f in plan.selected}
        new_snapshot_files = [fid for fid in new_manifest["snapshot"]["file_ids"]
                              if fid not in selected_ids]
        new_snapshot_files.append("compacted")
        if sealed_refs:
            new_snapshot_files.append("sealed-append")
            new_manifest["snapshot"]["visible_seq"] = max(
                plan.visible_seq_fence, max(r["seq"] for r in sealed_rows))
        new_manifest["snapshot"]["file_ids"] = new_snapshot_files
        # 受保护快照原封不动：仍引用其原始文件集与 visible_seq。
        new_manifest["selected_files"] = []

        manifest_path = out_dir / "manifest-after.json"
        manifest_path.write_text(json.dumps(new_manifest, ensure_ascii=False, indent=2),
                                 encoding="utf-8")

        # 提交对账（并发写者可在此注入）。
        seen = new_manifest
        if before_commit is not None:
            seen = before_commit(json.loads(json.dumps(new_manifest)), out_dir)
            if seen is not new_manifest:
                validate_manifest(seen)
        _reconcile(plan, seen)

        # 对账后还要保证并发清单里的额外文件都被新快照保留。
        planned_ids = {f["id"] for f in new_manifest["files"]}
        for f in seen["files"]:
            if f["id"] not in planned_ids:
                if f["id"] not in new_manifest["snapshot"]["file_ids"]:
                    new_manifest["snapshot"]["file_ids"].append(f["id"])
                new_manifest["files"].append(json.loads(json.dumps(f)))
                planned_ids.add(f["id"])
        # 并发写者推进的围栏（_reconcile 已保证不倒退）并入新快照。
        new_manifest["snapshot"]["visible_seq"] = max(
            new_manifest["snapshot"]["visible_seq"],
            seen["snapshot"]["visible_seq"])
        manifest_path.write_text(json.dumps(new_manifest, ensure_ascii=False, indent=2),
                                 encoding="utf-8")

        _assert_equivalent(manifest, root, new_manifest, out_dir, before_semantics, proof)

        report = {
            "plan": {
                "binding_fingerprint": plan.binding_fingerprint,
                "retention_version": plan.retention_version,
                "selected_file_ids": [f["id"] for f in plan.selected],
                "selected_identities": [
                    {"id": f["id"], "path": f["asset"]["path"],
                     "bytes": f["asset"]["bytes"], "sha256": f["asset"]["sha256"]}
                    for f in plan.selected],
                "protected_snapshot_ids": [s["id"] for s in plan.protected],
                "visible_seq_fence": plan.visible_seq_fence,
                "append_seqs": plan.append_seqs,
            },
            "coverage_proof": proof,
            "file_mapping": {
                "inputs": [f["id"] for f in plan.selected],
                "output": "compacted",
                "sealed_append": "sealed-append" if sealed_refs else None,
                "reclaimed": [],
                "note": "本次不物理回收对象，全部原文件保留，受保护快照仍读原文件",
            },
            "compacted_rows": {
                "kept": compacted["kept_rows"],
                "dropped": compacted["skipped"],
            },
            "per_key_folding": _per_key_folding(manifest, plan, root, budget, work),
            "equivalence_check": {
                "snapshots_checked": [s["snapshot_id"] for s in before_semantics],
                "result": "identical",
                "basis": ("受保护快照逐键比较 status/state/完整折叠轨迹；当前快照在原 "
                          "visible_seq 围栏上每键状态全等，轨迹差异仅限覆盖证明背书的"
                          "墓碑消去窗口，围栏前移仅可归因于计划内追加"),
            },
            "handle_peak": budget.peak,
            "manifest": str(manifest_path),
        }
        (out_dir / "compaction-report.json").write_text(
            json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        return report
    finally:
        shutil.rmtree(work, ignore_errors=True)


def _per_key_folding(manifest: dict, plan: Plan, root: Path, budget: HandleBudget,
                     tmpdir: Path) -> list[dict]:
    """输出每个键的状态由哪些 put/patch/delete 折叠而来（选中文件视角）。"""
    all_runs = file_runs(manifest, root)
    runs = [all_runs[f["id"]] for f in plan.selected]
    events = merged_events(runs, budget, manifest["schema"],
                           visible_seq=None, tmpdir=tmpdir)
    with tempfile.TemporaryDirectory(prefix="fold-", dir=tmpdir) as sd:
        results = list(fold(events, integer(manifest["memory_bytes"], 1),
                            Path(sd), budget))
        out = []
        for r in results:
            chain = []
            for chunk in r.provenance_chunks:
                if "inline" in chunk:
                    chain.extend(chunk["inline"])
                else:
                    with budget.opening(chunk["spill"]) as fh:
                        for line in fh:
                            chain.append(json.loads(line))
            out.append({
                "key": r.key,
                "status": r.status,
                "state": r.state,
                "folding": [{
                    "seq": e["seq"], "ordinal": e["ordinal"], "op": e["op"],
                    "effective": e["effective"], "sources": e["sources"],
                } for e in chain],
            })
        return out
