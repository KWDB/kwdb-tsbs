#  KWDB-tsbs

## 概述
kwdb-tsbs 是一个基于 Timescale/tsbs 改造的开源高性能时序数据库基准测试工具，完整支持 KWDB 的数据生成、导入和查询测试功能。

## 功能特性
- **数据生成**：支持自定义设备数、时间范围和数据间隔。
- **高效数据导入**：针对 KWDB 优化的批量写入工具。
- **查询场景**：提供标准时序查询模板。
- **自动化测试**：一键执行完整 KWDB 基准测试流程。

## 源码编译
1. 克隆仓库
```bash
git clone https://gitee.com/kwdb/kwdb-tsbs.git
cd kwdb-tsbs
```

2. 构建应用
```bash
make
```

编译成功后的相关二进制文件清单如下
```bash
kwdb-tsbs/
└── bin/
    ├── tsbs_generate_data      # 数据生成工具
    ├── tsbs_load_kwdb          # 数据导入工具
    ├── tsbs_generate_queries   # 查询执行工具
    └── tsbs_run_queries_kwdb   # 查询执行工具    
```

## 自动化测试脚本
* 推荐配置

| 组件    | 规格参数        |
|:------|:------------|
| CPU   | 16 核        |
| 内存    | 32 GB       |
| 磁盘    | SSD         |
| 操作系统    | ubuntu20.04 x86_64|
* 脚本路径
```bash
kwdb-tsbs/scripts/tsbs_kwdb.sh
```

* 执行脚本
```bash
workspace="$GOPATH/src/gitee.com/kwdb"  scripts/tsbs_kwdb.sh
```
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;参数说明<br>
&nbsp;&nbsp;&nbsp;&nbsp;workspace：KWDB 工作目录的路径。有关更多自动化测试脚本的参数配置，参见脚本文件中的详细注释说明。
* 运行完成后产生相关文件如下
```bash
kwdb-tsbs/
├── load_data/          # 产生的导入数据
├── query_data/         # 产生的查询数据
└── reports/            # 测试结果
    └── YMD_HMS_scale[scaleNum]_cluster[clusterNum]_insert[insertType]_wal[walSetting]_replica[replicaNum]_dop[degreeOfParallelism]/
        ├── load_data/  # 导入测试的结果
        └── query_data/ # 查询测试的结果
```
* 注意事项
  * 执行前请确保已正确安装 kwdb-tsbs 及其依赖
  * 脚本具有可执行权限(chmod +x scripts/tsbs_kwdb.sh)
  * 更多参数配置请直接查看脚本文件中的详细注释说明
  * 测试完成后，kwdb-tsbs 目录下会生成 reports 文件夹，记录测试结果

## 文档
* 有关 KWDB-tsbs 工具的详细参数说明与使用指南，请参阅技术文档：<br>
  kwdb_zh.md [(supplemental docs)](docs/kwdb_zh.md)
* 有关 TSBS MCP Server 的使用说明，请参阅：<br>
  tsbs_mcp_server_zh.md [(supplemental docs)](docs/tsbs_mcp_server_zh.md)
* 有关其他数据库的基准测试说明，请参阅通用文档:<br>
  README.md [(supplemental docs)](docs/README.md)

## 许可证声明
本项目基于 [timescale/tsbs](https://github.com/timescale/tsbs) 开发适配，保留原始项目版权声明并延用 MIT 许可证。

## 核心改进
* 新增功能
  * 新增 KWDB 全套支持
  * 添加 KWDB 自动化脚本

* 文档完善
  * 添加 KWDB 相关中英文版 README

