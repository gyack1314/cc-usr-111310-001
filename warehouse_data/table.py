"""按键归并读取。

记录按 (key, seq, ordinal) 构成严格总序；文件内部按该序排序，文件之间可重叠。
本模块只依赖标准库，提供：

* ``validate_record``：按 docs/data-contract.md 校验单条记录形状；
* ``HandleBudget``：统计同时打开的输入/临时归并段句柄，超出预算即报错；
* ``merged_events``：有界多路归并，句柄数与单键内存占用均有上界；
* ``read_snapshot``：折叠固定快照（文件清单 + visible_seq），输出每键状态及
  由哪些 put/patch/delete 折叠而来的溯源链。

语义要点：put 给完整行并可在 delete 后重建行；patch 只改显式出现的字段，
字段缺席与显式 null 不同；没有活跃行时 patch 只记未生效证据，不创建部分行；
delete 在无行时仍留墓碑，之后的 patch 不能复活，只有完整 put 能重建。
"""
from __future__ import annotations

import hashlib
import heapq
import json
import os
import shutil
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path

from .io import asset, integer

PUT = "put"
PATCH = "patch"
DELETE = "delete"
_OPS = frozenset((PUT, PATCH, DELETE))

# 受契约固定的列类型：station 为不可 null 的 string，temperature 为 int 或 null。
_COLUMN_RULES = {
    "station": ("string", False),
    "temperature": ("int", True),
}

LIVE = "live"
DELETED = "deleted"
ABSENT = "absent"


class TableError(ValueError):
    """湖表形状或预算错误。"""


class MergeConflict(TableError):
    """同一 (key, seq, ordinal) 出现不同字节的记录。"""


def canonical(rec: dict) -> str:
    """记录的规范字节表示：同序重放据此判同字节，不同即冲突。"""
    return json.dumps(rec, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def _check_column(column: str, value, nullable: bool) -> None:
    kind, _ = _COLUMN_RULES.get(column, ("string", False))
    if value is None:
        if not nullable:
            raise TableError(f"列 {column} 不允许 null")
        return
    if kind == "string":
        if not isinstance(value, str) or not value:
            raise TableError(f"列 {column} 必须是非空字符串")
    elif kind == "int":
        if type(value) is not int:
            raise TableError(f"列 {column} 必须是整数")
    else:  # pragma: no cover - 规则表内置，不会走到
        raise TableError(f"列 {column} 类型未知")


def validate_record(rec: object, schema: list[str]) -> dict:
    """校验单条记录，返回只含允许键的规范化副本。"""
    if not isinstance(rec, dict):
        raise TableError("记录必须是对象")
    allowed = {"key", "seq", "ordinal", "op", "value"}
    if set(rec) - allowed:
        raise TableError("记录含未知字段")
    key = rec.get("key")
    if not isinstance(key, str) or not key:
        raise TableError("key 必须是非空字符串")
    seq = rec.get("seq")
    ordinal = rec.get("ordinal")
    if type(seq) is not int or seq < 0 or type(ordinal) is not int or ordinal < 0:
        raise TableError("seq/ordinal 必须是非负整数")
    op = rec.get("op")
    if op not in _OPS:
        raise TableError("op 必须是 put/patch/delete")
    schema_set = frozenset(schema)
    if op == DELETE:
        if "value" in rec:
            raise TableError("delete 不能携带 value")
        return {"key": key, "seq": seq, "ordinal": ordinal, "op": DELETE}
    value = rec.get("value")
    if not isinstance(value, dict) or not value:
        raise TableError(f"{op} 必须携带非空 value")
    fields = frozenset(value)
    if op == PUT:
        if fields != schema_set:
            raise TableError("完整 put 必须含全部 schema 字段且无额外字段")
    elif not fields <= schema_set:
        raise TableError("patch 只能包含 schema 内字段")
    for column in fields:
        _check_column(column, value[column], nullable=_COLUMN_RULES[column][1])
    return {"key": key, "seq": seq, "ordinal": ordinal, "op": op,
            "value": {c: value[c] for c in schema if c in value}}


def validate_manifest(v: dict) -> None:
    """清单级形状与预算校验（文件身份由调用方用 io.asset 核对）。"""
    if v.get("kind") != "merge-on-read":
        raise TableError("kind 必须是 merge-on-read")
    schema = v.get("schema")
    if not isinstance(schema, list) or not schema or any(
            not isinstance(c, str) or not c for c in schema) or len(set(schema)) != len(schema):
        raise TableError("schema 必须是非空且不重复的字符串列表")
    for c in schema:
        if c not in _COLUMN_RULES:
            raise TableError(f"列 {c} 缺少类型规则")
    integer(v["memory_bytes"], 1)
    if type(v["max_open_files"]) is not int or v["max_open_files"] < 3:
        raise TableError("max_open_files 至少为 3（输入与临时归并段共享预算）")
    ids = {f["id"] for f in v["files"]}
    if len(ids) != len(v["files"]):
        raise TableError("file id 重复")

    def check_snapshot(s: dict) -> None:
        if not isinstance(s.get("id"), str) or not s["id"]:
            raise TableError("快照 id 必须是非空字符串")
        integer(s["visible_seq"], 0)
        fids = s.get("file_ids")
        if not isinstance(fids, list) or not fids or len(set(fids)) != len(fids):
            raise TableError("快照 file_ids 必须是非空且不重复的列表")
        if not set(fids) <= ids:
            raise TableError("快照引用了未知文件；快照只引用显式 file_ids，不自动纳入目录文件")

    check_snapshot(v["snapshot"])
    seen = set()
    for s in v.get("protected_snapshots", []):
        check_snapshot(s)
        if s["id"] in seen:
            raise TableError("受保护快照 id 重复")
        seen.add(s["id"])
    selected = v.get("selected_files", [])
    if not isinstance(selected, list) or not set(selected) <= ids:
        raise TableError("selected_files 必须是已知文件 id 的列表")
    if not isinstance(v.get("retention_version"), str) or not v["retention_version"]:
        raise TableError("retention_version 必须是非空字符串")
    appends = v.get("append_during_compaction", [])
    if not isinstance(appends, list):
        raise TableError("append_during_compaction 必须是列表")
    for rec in appends:
        validate_record(rec, schema)


class HandleBudget:
    """同时打开句柄的硬预算，输入文件与临时归并段一起计数。"""

    def __init__(self, limit: int):
        if limit < 3:
            raise TableError("max_open_files 至少为 3")
        self.limit = limit
        self.open = 0
        self.peak = 0

    @contextmanager
    def opening(self, path, mode: str = "rb"):
        if self.open >= self.limit:
            raise TableError(f"打开文件数超过预算 {self.limit}")
        fh = open(path, mode)
        self.open += 1
        self.peak = max(self.peak, self.open)
        try:
            yield fh
        finally:
            fh.close()
            self.open -= 1


@dataclass
class Run:
    """一个已排序的 JSONL 归并段。

    原始数据文件每行就是一条记录；压实/归并产生的临时段行为包装对象
    ``{"r": 记录, "s": 来源文件 id 列表}``，以便同序重放跨段保留全部来源。
    """

    path: Path
    spill: bool
    source: str | None = None

    def parse_line(self, line: bytes):
        obj = json.loads(line.decode("utf-8"))
        if self.spill:
            return obj["r"], list(obj["s"])
        return obj, [self.source]


@dataclass
class Event:
    key: str
    seq: int
    ordinal: int
    rec: dict
    sources: list[str]

    @property
    def triple(self):
        return self.key, self.seq, self.ordinal


def _iter_run(run: Run, budget: HandleBudget, schema: list[str], visible_seq: int | None):
    prev = None
    with budget.opening(run.path) as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            obj, sources = run.parse_line(line)
            rec = validate_record(obj, schema)
            if visible_seq is not None and rec["seq"] > visible_seq:
                continue
            triple = (rec["key"], rec["seq"], rec["ordinal"])
            if prev is not None and triple <= prev:
                raise TableError(
                    f"文件 {run.path} 未按 (key, seq, ordinal) 严格升序，或存在重复总序")
            prev = triple
            yield Event(rec["key"], rec["seq"], rec["ordinal"], rec, sources)


class _HeapItem:
    __slots__ = ("triple", "idx", "event")

    def __init__(self, event: Event, idx: int):
        self.triple = event.triple
        self.idx = idx
        self.event = event

    def __lt__(self, other: "_HeapItem"):
        if self.triple != other.triple:
            return self.triple < other.triple
        return self.idx < other.idx


def _merge_group_to_spill(runs: list[Run], budget: HandleBudget, schema: list[str],
                          visible_seq: int | None, spill_path: Path) -> Run:
    """把一组归并段合并成一个临时段，句柄峰值为 len(runs)+1。"""
    iters = [_iter_run(r, budget, schema, visible_seq) for r in runs]
    heap: list[_HeapItem] = []
    for idx, it in enumerate(iters):
        try:
            heapq.heappush(heap, _HeapItem(next(it), idx))
        except StopIteration:
            pass
    with budget.opening(spill_path, "wb") as out:
        while heap:
            head = heapq.heappop(heap)
            event = head.event
            same = [event]
            while heap and heap[0].triple == event.triple:
                other = heapq.heappop(heap).event
                if canonical(other.rec) != canonical(event.rec):
                    raise MergeConflict(
                        f"总序 {event.triple} 存在不同字节的冲突记录")
                same.append(other)
            sources: list[str] = []
            for ev in same:
                for sid in ev.sources:
                    if sid not in sources:
                        sources.append(sid)
            out.write((json.dumps({"r": event.rec, "s": sources},
                                  ensure_ascii=False) + "\n").encode("utf-8"))
            it = iters[head.idx]
            try:
                heapq.heappush(heap, _HeapItem(next(it), head.idx))
            except StopIteration:
                pass
    return Run(spill_path, spill=True)


def merged_events(runs: list[Run], budget: HandleBudget, schema: list[str],
                  visible_seq: int | None = None, tmpdir: Path | None = None):
    """有界多路归并，按严格总序产出 :class:`Event`。

    段数超过 ``max_open_files-1`` 时做多趟外部归并，每趟同时打开的句柄不超过
    预算；同总序记录要求规范字节完全相同，否则抛 :class:`MergeConflict`。
    """
    runs = list(runs)
    scratch: list[Path] = []
    fan_in = budget.limit - 1  # 留出一个句柄给临时段/输出
    while len(runs) > fan_in:
        next_runs: list[Run] = []
        for start in range(0, len(runs), fan_in):
            group = runs[start:start + fan_in]
            fd, name = tempfile.mkstemp(prefix="merge-", suffix=".jsonl", dir=tmpdir)
            os.close(fd)
            scratch.append(Path(name))
            next_runs.append(_merge_group_to_spill(
                group, budget, schema, visible_seq, Path(name)))
        runs = next_runs

    iters = [_iter_run(r, budget, schema, visible_seq) for r in runs]
    heap: list[_HeapItem] = []
    for idx, it in enumerate(iters):
        try:
            heapq.heappush(heap, _HeapItem(next(it), idx))
        except StopIteration:
            pass
    try:
        while heap:
            head = heapq.heappop(heap)
            event = head.event
            while heap and heap[0].triple == event.triple:
                other = heapq.heappop(heap).event
                if canonical(other.rec) != canonical(event.rec):
                    raise MergeConflict(f"总序 {event.triple} 存在不同字节的冲突记录")
                for sid in other.sources:
                    if sid not in event.sources:
                        event.sources.append(sid)
            yield event
            it = iters[head.idx]
            try:
                heapq.heappush(heap, _HeapItem(next(it), head.idx))
            except StopIteration:
                pass
    finally:
        for p in scratch:
            try:
                p.unlink()
            except FileNotFoundError:
                pass


@dataclass
class KeyResult:
    key: str
    status: str
    state: dict | None
    # 每个元素是 {"inline": [溯源条目...]} 或 {"spill": 路径, "count": n}
    provenance_chunks: list[dict] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {"key": self.key, "status": self.status, "state": self.state,
                "provenance_chunks": self.provenance_chunks}


class SnapshotRead:
    """一次快照读取的句柄，关闭时清理溯源临时段。"""

    def __init__(self, snapshot: dict, results: list[KeyResult], spill_dir: Path,
                 budget: HandleBudget):
        self.snapshot = snapshot
        self.results = results
        self._spill_dir = spill_dir
        self.budget = budget

    def iter_provenance(self, result: KeyResult):
        """流式回放某键的完整溯源链，超大键组不会被整组载入内存。"""
        for chunk in result.provenance_chunks:
            if "inline" in chunk:
                yield from chunk["inline"]
            else:
                with self.budget.opening(chunk["spill"]) as fh:
                    for line in fh:
                        yield json.loads(line)

    def trace(self, result: KeyResult) -> list[tuple]:
        """与文件身份无关的折叠轨迹，用于压实前后等价比对。"""
        return [(e["seq"], e["ordinal"], e["op"], e["effective"])
                for e in self.iter_provenance(result)]

    def close(self) -> None:
        shutil.rmtree(self._spill_dir, ignore_errors=True)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


def _entry_size(entry: dict) -> int:
    return len(json.dumps(entry, ensure_ascii=False).encode("utf-8"))


def fold(events, memory_bytes: int, spill_dir: Path, budget: HandleBudget):
    """流式折叠事件流，单键历史超过 memory_bytes 时分段落盘，不整组 list。"""
    current_key = None
    status = ABSENT
    state: dict | None = None
    chunks: list[dict] = []
    inline: list[dict] = []
    inline_bytes = 0
    chunk_seq = 0

    def flush_inline():
        nonlocal inline, inline_bytes, chunk_seq
        if not inline:
            return
        fd, name = tempfile.mkstemp(prefix=f"prov-{current_key}-{chunk_seq}-",
                                    suffix=".jsonl", dir=spill_dir)
        os.close(fd)
        path = Path(name)
        with budget.opening(path, "wb") as fh:
            for entry in inline:
                fh.write((json.dumps(entry, ensure_ascii=False) + "\n").encode("utf-8"))
        chunks.append({"spill": str(path), "count": len(inline)})
        chunk_seq += 1
        inline = []
        inline_bytes = 0
    def emit() -> KeyResult:
        flush_inline()
        return KeyResult(current_key, status,
                         None if state is None else dict(state), chunks)

    for ev in events:
        if ev.key != current_key:
            if current_key is not None:
                yield emit()
            current_key = ev.key
            status = ABSENT
            state = None
            chunks = []
            inline = []
            inline_bytes = 0
            chunk_seq = 0
        rec = ev.rec
        op = rec["op"]
        effective = False
        if op == PUT:
            state = dict(rec["value"])
            status = LIVE
            effective = True
        elif op == PATCH:
            if status == LIVE:
                for column, value in rec["value"].items():
                    state[column] = value  # 显式 null 也覆盖；缺席字段保持原值
                effective = True
            # 无活跃行（absent/deleted）：只记未生效证据，不创建部分行
        else:  # DELETE
            if status != DELETED:
                status = DELETED
                state = None
                effective = True  # 无行时 delete 仍建立墓碑
        entry = {"sources": list(ev.sources), "seq": rec["seq"],
                 "ordinal": rec["ordinal"], "op": op, "effective": effective}
        if "value" in rec:
            entry["value"] = rec["value"]
        inline.append(entry)
        inline_bytes += _entry_size(entry)
        if inline_bytes >= memory_bytes:
            flush_inline()
    if current_key is not None:
        yield emit()


def file_runs(manifest: dict, root: Path) -> dict[str, Run]:
    """核对全部文件身份并构造原始归并段（文件内部已按总序排序）。"""
    runs: dict[str, Run] = {}
    for f in manifest["files"]:
        raw = asset(root, f["asset"])
        if f["asset"].get("rows") is not None:
            if raw.count(b"\n") != integer(f["asset"]["rows"], 0):
                raise TableError(f"文件 {f['id']} 行数与资产身份不符")
        runs[f["id"]] = Run(root / f["asset"]["path"], spill=False, source=f["id"])
    return runs


def resolve_snapshot(manifest: dict, snapshot_id: str | None) -> dict:
    """按 id 解析当前快照或受保护快照；id 为 None 时取当前快照。"""
    if snapshot_id is None:
        return manifest["snapshot"]
    if snapshot_id == manifest["snapshot"]["id"]:
        return manifest["snapshot"]
    for s in manifest.get("protected_snapshots", []):
        if s["id"] == snapshot_id:
            return s
    raise TableError(f"未知快照 {snapshot_id}")


def read_snapshot(manifest: dict, root: Path, snapshot_id: str | None = None,
                  *, budget: HandleBudget | None = None) -> SnapshotRead:
    """读取固定快照：只折叠其 file_ids 中 seq 不晚于 visible_seq 的记录。"""
    validate_manifest(manifest)
    snapshot = resolve_snapshot(manifest, snapshot_id)
    budget = budget or HandleBudget(integer(manifest["max_open_files"], 3))
    memory_bytes = integer(manifest["memory_bytes"], 1)
    all_runs = file_runs(manifest, root)
    runs = [all_runs[fid] for fid in snapshot["file_ids"]]
    spill_dir = Path(tempfile.mkdtemp(prefix="snapshot-read-"))
    try:
        events = merged_events(runs, budget, manifest["schema"],
                               visible_seq=snapshot["visible_seq"], tmpdir=spill_dir)
        results = list(fold(events, memory_bytes, spill_dir, budget))
    except BaseException:
        shutil.rmtree(spill_dir, ignore_errors=True)
        raise
    return SnapshotRead(snapshot, results, spill_dir, budget)


def fingerprint(binding: dict) -> str:
    """对计划绑定内容取稳定指纹，便于报告与日志对账。"""
    raw = json.dumps(binding, sort_keys=True, ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()
