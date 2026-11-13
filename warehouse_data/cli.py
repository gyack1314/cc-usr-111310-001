"""湖表工具命令行：资料摘要、按键归并读取、在线压实。"""
import argparse
import json
import shutil
from pathlib import Path

from .compaction import execute_compaction
from .domain import summarize
from .io import load
from .table import read_snapshot


def _cmd_inspect(args) -> None:
    v, digest = load(args.file)
    print(json.dumps({"manifest_sha256": digest,
                      "summary": summarize(v, Path(args.file).parent)},
                     ensure_ascii=False, indent=2))


def _cmd_read(args) -> None:
    v, _ = load(args.file)
    root = Path(args.file).parent
    with read_snapshot(v, root, args.snapshot) as rd:
        payload = {
            "snapshot_id": rd.snapshot["id"],
            "visible_seq": rd.snapshot["visible_seq"],
            "file_ids": rd.snapshot["file_ids"],
            "handle_peak": rd.budget.peak,
            "keys": [{
                "key": r.key,
                "status": r.status,
                "state": r.state,
                "folding": [{
                    "seq": e["seq"], "ordinal": e["ordinal"], "op": e["op"],
                    "effective": e["effective"], "sources": e["sources"],
                    **({"value": e["value"]} if "value" in e else {}),
                } for e in rd.iter_provenance(r)],
            } for r in rd.results],
        }
    print(json.dumps(payload, ensure_ascii=False, indent=2))


def _cmd_compact(args) -> None:
    v, _ = load(args.file)
    root = Path(args.file).parent
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    # 本次不物理回收对象：把全部原始资产复制到输出目录，受保护快照仍读原文件副本。
    for f in v["files"]:
        shutil.copy2(root / f["asset"]["path"], out / f["asset"]["path"])
    report = execute_compaction(v, out, out)
    print(json.dumps(report, ensure_ascii=False, indent=2))


def main() -> None:
    p = argparse.ArgumentParser(prog="warehouse_data")
    sub = p.add_subparsers(dest="command", required=True)

    q = sub.add_parser("inspect", help="只读原始资料并输出摘要")
    q.add_argument("file")
    q.set_defaults(func=_cmd_inspect)

    q = sub.add_parser("read", help="按快照做按键归并读取，输出每键状态与折叠溯源")
    q.add_argument("file")
    q.add_argument("--snapshot", default=None, help="快照 id，默认当前快照")
    q.set_defaults(func=_cmd_read)

    q = sub.add_parser("compact", help="在线压实选中文件并生成新清单（不回收旧对象）")
    q.add_argument("file")
    q.add_argument("--out", required=True, help="产物与新清单输出目录")
    q.set_defaults(func=_cmd_compact)

    a = p.parse_args()
    a.func(a)


if __name__ == "__main__":
    main()
