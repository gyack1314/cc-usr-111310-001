# 设备状态湖表的部分更新归并与安全压实

此项目保存生产与供应链协同的湖仓原始资料。`warehouse_data.io`读取JSON清单及核对文件身份，`warehouse_data.domain`检查基本形状并输出数量，`warehouse_data.table`做按键归并读取（严格总序、有界多路归并、put/patch/delete折叠），`warehouse_data.compaction`做在线压实与覆盖证明，`warehouse_data.cli`展示摘要、读取与压实报告。[资料约定](docs/data-contract.md)定义字段、类型、时间、数量和版本范围。设备、批次、组织、事件和成本均为合成，不连接真实工厂或生产仓库。

Python3.11以上，无第三方Python依赖。项目根目录运行：

```sh
python -m unittest discover -s tests -v
python -m compileall -q warehouse_data tests
python -m warehouse_data.cli inspect data/normal.json
python -m warehouse_data.cli read data/normal.json
python -m warehouse_data.cli read data/normal.json --snapshot s2
python -m warehouse_data.cli compact data/normal.json --out out/
```

`inspect`只读原始资料，不生成仓库新版本或计算领域结果。`read`按快照固定的文件清单与`visible_seq`做按键归并：patch只改显式出现的字段（缺席与显式null不同），无活跃行时patch只记未生效证据，delete留墓碑且后续patch不能复活，只有完整put能重建行；输出每键状态及由哪些put/patch/delete折叠而来的溯源链。归并受`max_open_files`（输入与临时归并段共享）和`memory_bytes`（单键历史超预算分段落盘，不整组载入）双预算约束。

`compact`把`selected_files`压实为单个有序文件并生成`manifest-after.json`与`compaction-report.json`：计划绑定所选文件身份、受保护快照集合、`retention_version`与版本围栏；计算阶段允许其他写者追加，提交时只替换仍存在且身份相同的选中文件，并发追加封存为新文件并保留，保留策略或受保护集合变化则拒绝并要求重建计划。墓碑只有在证明该键更早版本不可能留在任何未选文件、且墓碑之后存在完整put重建时才可消去；本次不物理回收对象，全部原文件保留，受保护快照仍读其原文件。压实前后对每个受保护`visible_seq`逐键比对状态与折叠轨迹，不能只比最终行数。

`invalid-config.json`有基本形状错误；其余清单有完整资料身份，但可能包含需要业务操作判断的冲突、缺口或不满足条件的边界。null与缺字段分别保留，有理数和decimal用字符串存储，不能先经float丢精度。

Dockerfile固定Linux镜像，`docker build -t warehouse-material .`构建；`docker run --rm warehouse-material`显示摘要；`docker run --rm warehouse-material python -m unittest discover -s tests -v`运行已有检查。所有资料本地可读，不需要账号、外部数据库或在线数据源。
