"""核对原始配置及数量，不执行仓库变换。"""
from .io import asset,integer,unique
def summarize(v,root):
    ids=unique(v['files'],'id');integer(v['memory_bytes'],1);integer(v['max_open_files'],3)
    for f in v['files']:asset(root,f['asset'])
    if not set(v['snapshot']['file_ids'])<=ids:raise ValueError('快照文件未知')
    return {'files':len(ids),'protected_reads':len(v['protected_snapshots']),'selected_files':len(v['selected_files'])}
