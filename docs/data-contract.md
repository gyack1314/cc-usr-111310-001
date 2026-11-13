# 有序状态文件

每文件JSONL按Unicode key、seq、ordinal升序。字段schema中station为string，temperature为int或null，完整put必须含全部字段且无额外字段；patch只更新存在的字段，允许temperature显式null，station不可null。seq/ordinal为非负整数，总序冲突按(key,seq,ordinal)判断，不同key同序号不冲突。没有活跃put时patch记为未生效证据，不创建部分行；delete在无行时仍留墓碑。

snapshot只引用file_ids，visible_seq按提交过滤，不能把目录文件自动加入。protected_snapshots各有独立文件集；压实可保留全部必要历史事件，不要求为减少行数牺牲历史可读。基准put可以作为带有效起点的完整状态，但不得让其越过受保护读边界。墓碑消去须覆盖全部更早来源的键范围证据。

max_open_files包含输入和临时归并段；超大键组可流式折叠并分段保存受保护版本，不整组载入。append_during_compaction是可控调度点中的可信新增事件，封存为新文件后需保留。本次不回收旧文件。初始代码只检查文件身份、数量及资源预算。

## 读取与压实实现约定（warehouse_data.table / warehouse_data.compaction）

严格总序为 `(key, seq, ordinal)`；同总序记录的规范字节（键排序、紧凑分隔的JSON）完全相同才视为同序重放并合并来源，字节不同即 `MergeConflict`，不同key同序号不冲突。读取只折叠快照 `file_ids` 中 `seq <= visible_seq` 的记录，目录里存在但快照不引用的文件永不自动纳入。多路归并在文件数超过 `max_open_files-1` 时做分趟外部归并，每趟同时打开句柄不超过预算；单键溯源超过 `memory_bytes` 时按段落盘并可流式回放，不整组 list，也不对每列分别取最大值。

折叠语义：`put` 给完整行（schema 全字段、无额外字段），可在 delete 后重建；`patch` 只覆盖显式出现的字段，显式 null 覆盖原值、缺席字段保持原值；无活跃行（从未 put 或已 delete）时 patch 标 `effective=false` 作为未生效证据，不创建部分行；delete 无行时仍建立墓碑，其后的 patch 不能复活，只有完整 put 能重建。读取结果按 key 给出 `live/deleted/absent` 状态、当前行与完整溯源链（seq、ordinal、op、effective、来源文件、patch 显式值）。

压实计划绑定选中文件身份（path/bytes/sha256）、受保护快照集合、`retention_version` 与当前 `visible_seq` 围栏。提交对账 (`_reconcile`) 要求：选中文件仍存在且身份相同（否则拒绝），保留策略与受保护快照集合未变（变化即要求重建计划），快照围栏不倒退；对账中发现的额外文件全部并入新快照与文件表。`append_during_compaction` 事件经形状与排序校验后封存为独立有序文件，冲突按严格总序规则在归并中检出。

墓碑消去需同时满足：覆盖证明（扫描全部未选文件，同键最小 seq 不存在或大于墓碑 seq，即更早版本不可能留在任何未选文件；不能用“选中文件里没有旧行”代替全快照覆盖证明）与墓碑之后在选中范围内存在完整 put 重建。消去时墓碑及墓碑到下一个完整 put 之间的未生效记录一并跳过，并在报告 `compacted_rows.dropped` 中逐条列明理由；不满足任一条件的墓碑原样保留。受保护快照的文件集与围栏原样保留，本次不物理回收任何对象（`file_mapping.reclaimed` 恒为空）。压实报告 `compaction-report.json` 包含计划绑定指纹、每键折叠链、未选文件覆盖证明、新旧文件映射与等价校验结果；等价校验对每个受保护快照逐键比较 status/state/完整折叠轨迹，对当前快照在原围栏比较每键状态并把轨迹差异限制在有证明背书的消去窗口内，围栏前移只可归因于计划内追加。
