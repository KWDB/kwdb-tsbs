# TSBS 补充指南：KWDB-tsbs

KWDB 是一款面向 AIoT 物联网场景的分布式多模数据库产品，支持在同一实例同时建立时序库和关系库并融合处理多模数据，具备千万级设备接入、百万
级数据秒级写入、亿级数据秒级读取等时序数据高效处理能力，具有稳定安全、高可用、易运维等特点。

本文档是对主 README_zh 的补充，详细说明以下内容：

* 数据产生工具 (`tsbs_generate_data`) 相关使用。
* 数据导入工具 (`tsbs_load_kwdb`) 相关使用。
* 查询产生工具 (`tsbs_generate_queries`) 相关使用。
* 查询执行工具 (`tsbs_run_queries_kwdb`) 相关使用。

**请务必先阅读主 README_zh [(supplemental docs)](../README_zh.md) 文档**

## 数据格式
tsbs_generate_data 为 KWDB 生成的数据采用“伪 CSV”格式。每行表示一条记录，首项为操作类型（1 或 3）：

- 3 表示写入标签值，格式为：
  - `3,表名,ptag名,属性值`
- 1 表示插入数据（含数据值和标签值），格式为：
  - `1,ptag名,字段数量,插入数据`


以 cpu-only 场景为例：

```text
3,cpu,host_0,('host_0','eu-central-1','eu-central-1a','6','Ubuntu15.10','x86','SF','19','1','test')
1,host_0,11,(1451606400000,58,2,24,61,22,63,6,44,80,38,'host_0')
```


---
## `tsbs_generate_data`  附加参数
```bash
`--use-case="cpu-only" --seed=123 --scale=100 --log-interval="10s" --timestamp-start="2016-01-01T00:00:00Z" --timestamp-end="2016-02-01T00:00:00Z" --format="kwdb" --orderquantity=12`
```

#### `-use-case` （类型：`string`，默认值：`cpu-only`）
cpu-only/IoT

#### `-scale` （类型：`int`）
设备数量。注意：部分查询需至少 10 台设备才能满足条件

#### `-log-interval` （类型：`string`）
数据采样间隔时间

#### `-timestamp-start` （类型：`string`）
数据采样开始时间（默认 2016-01-01T00:00:00Z）

#### `-timestamp-end` （类型：`string`）
数据采样结束时间

#### `-format` （类型：`string`）
类型:kwdb

#### `-orderquantity` （类型：`int`，默认值：`12`）
设备生成顺序，例如 100 台设备时，format = 12 会首先生成 host_0 到 host_11，接着生成 host_12 到 host_23，依此类推

---
## `tsbs_load_kwdb` 附加参数
```bash
` --file=./data.dat --user=root --pass=1234 --host=127.0.0.1 --port=26257 --insert-type=insert --db-name=benchmark --case=cpu-only --batch-size=1000 --workers=12 --partition=false `
```

### 连接相关
#### `-user` （类型：`string`，默认值：`root`）
KWDB 登录用户名。

#### `-pass` （类型：`string`，默认值：`空`）
用户密码。

#### `-host` （类型：`string`）
KWDB 服务器地址。

#### `-port` （类型：`int`，默认值：`26257`）
KWDB 服务器端口。

#### `-insert-type` （类型：`string`）
可选值：insert、prepare 或 prepareiot。

注：case 与 insert-type 的对应关系如下：

| case     | insert-type |
|----------|-------------|
| cpu-only | insert      |
| cpu-only | prepare     |
| IoT      | insert      |
| IoT      | prepareiot  |


#### `-db-name` （类型：`string`）
目标数据库名。

#### `-case` （类型：`string`，默认值：`cpu-only`）
cpu-only/iot

#### `-batch-size/-preparesize` （类型：`int`）
每批次写入的数据量。若使用 --insert-type=prepare，需替换为 --preparesize。

#### `-workers` （类型：`int`）
并发写入数，建议与 tsbs_generate_data 的 orderquantity 保持一致。

#### `-partition` （类型：`bool`）
单节点设为 false，集群设为 true。

---
## `tsbs_generate_queries` 附加参数
`--use-case="cpu-only" --seed=123 --scale=100 --query-type="single-groupby-1-8-1" --format="kwdb" --queries=10 --db-name=benchmark --timestamp-start="2016-01-01T00:00:00Z" --timestamp-end="2016-01-05T00:00:01Z" --prepare=false`

#### `-use-case` （类型：`string`，默认值：`cpu-only`）
cpu-only/IoT

#### `-query-type` （类型：`string`）
查询类型

#### `-prepare` （类型：`bool`, default: `false`）
是否使用模板查询

##### cpu-only
| Query type            | Description                                                                                                       |
|:----------------------|:------------------------------------------------------------------------------------------------------------------|
| single-groupby-1-1-1  | Simple aggregate (MAX) on one metric for 1 host, every 5 mins for 1 hour                                         |
| single-groupby-1-1-12 | Simple aggregate (MAX) on one metric for 1 host, every 5 mins for 12 hours                                       |
| single-groupby-1-8-1  | Simple aggregate (MAX) on one metric for 8 hosts, every 5 mins for 1 hour                                        |
| single-groupby-5-1-1  | Simple aggregate (MAX) on 5 metrics for 1 host, every 5 mins for 1 hour                                          |
| single-groupby-5-1-12 | Simple aggregate (MAX) on 5 metrics for 1 host, every 5 mins for 12 hours                                        |
| single-groupby-5-8-1  | Simple aggregate (MAX) on 5 metrics for 8 hosts, every 5 mins for 1 hour                                         |
| cpu-max-all-1         | Aggregate across all CPU metrics per hour over 1 hour for a single host                                           |
| cpu-max-all-8         | Aggregate across all CPU metrics per hour over 1 hour for eight hosts                                             |
| double-groupby-1      | Aggregate on across both time and host, giving the average of 1 CPU metric per host per hour for 24 hours         |
| double-groupby-5      | Aggregate on across both time and host, giving the average of 5 CPU metrics per host per hour for 24 hours        |
| double-groupby-all    | Aggregate on across both time and host, giving the average of all (10) CPU metrics per host per hour for 24 hours |
| high-cpu-all          | All the readings where one metric is above a threshold across all hosts                                           |
| high-cpu-1            | All the readings where one metric is above a threshold for a particular host                                      |
| lastpoint             | The last reading for each host                                                                                    |
| groupby-orderby-limit | The last 5 aggregate readings (across time) before a randomly chosen endpoint                                     |

### IoT
| Query type                        | Description                                                                             |
|:----------------------------------|:----------------------------------------------------------------------------------------|
| last-loc                          | Fetch real-time (i.e. last) location of each truck                                      |
| low-fuel                          | Fetch all trucks with low fuel (less than 10%)                                          |
| high-load                         | Fetch trucks with high current load (over 90% load capacity)                            |
| stationary-trucks                 | Fetch all trucks that are stationary (low avg velocity in last 10 mins)                 |
| long-driving-sessions             | Get trucks which haven't rested for at least 20 mins in the last 4 hours                |
| long-daily-sessions               | Get trucks which drove more than 10 hours in the last 24 hours                          |
| avg-vs-projected-fuel-consumption | Calculate average vs. projected fuel consumption per fleet                              |
| avg-daily-driving-duration        | Calculate average daily driving duration per driver                                     |
| avg-daily-driving-session         | Calculate average daily driving session per driver                                      |
| avg-load                          | Calculate average load per truck model per fleet                                        |
| daily-activity                    | Get the number of hours truck has been active (vs. out-of-commission) per day per fleet |
| breakdown-frequency               | Calculate breakdown frequency by truck model                                            |

#### `-queries` （类型：`int`）
生成的查询总数。

---
## `tsbs_run_queries_kwdb` 附加参数
`--file=./query.dat --host=127.0.0.1 --port=26257 --user=root --pass=1234 -workers=1 --prepare=false --query-type="single-groupby-1-8-1"`

#### `-user` （类型：`string`，默认值：`root`）
KWDB 登录用户名。

#### `-pass` （类型：`string`，默认值：`空`）
用户密码。

#### `-host` （类型：`string`）
KWDB 服务器地址。

#### `-port` （类型：`int`，默认值：`26257`）
KWDB 服务器端口。

#### `-query-type` （类型：`string`）
查询类型

#### `-prepare` （类型：`bool`）
是否使用模板查询(和产生查询时prepare保持一致)