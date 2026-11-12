# 设备状态湖表的部分更新归并与安全压实

此项目保存生产与供应链协同的湖仓原始资料。`warehouse_data.io`读取JSON清单及核对文件身份，`warehouse_data.domain`检查基本形状并输出数量，`warehouse_data.cli`展示摘要。[资料约定](docs/data-contract.md)定义字段、类型、时间、数量和版本范围。设备、批次、组织、事件和成本均为合成，不连接真实工厂或生产仓库。

Python3.11以上，无第三方Python依赖。项目根目录运行：

```sh
python -m unittest discover -s tests -v
python -m compileall -q warehouse_data tests
python -m warehouse_data.cli inspect data/normal.json
```

`inspect`只读原始资料，不生成仓库新版本或计算领域结果。`invalid-config.json`有基本形状错误；其余清单有完整资料身份，但可能包含需要业务操作判断的冲突、缺口或不满足条件的边界。null与缺字段分别保留，有理数和decimal用字符串存储，不能先经float丢精度。

Dockerfile固定Linux镜像，`docker build -t warehouse-material .`构建；`docker run --rm warehouse-material`显示摘要；`docker run --rm warehouse-material python -m unittest discover -s tests -v`运行已有检查。所有资料本地可读，不需要账号、外部数据库或在线数据源。
