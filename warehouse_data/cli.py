"""展示原始仓库资料数量。"""
import argparse,json
from pathlib import Path
from .io import load
from .domain import summarize

def main():
    p=argparse.ArgumentParser();p.add_argument('command',choices=['inspect']);p.add_argument('file');a=p.parse_args()
    v,digest=load(a.file);print(json.dumps({'manifest_sha256':digest,'summary':summarize(v,Path(a.file).parent)},ensure_ascii=False,indent=2))
if __name__=='__main__':main()
