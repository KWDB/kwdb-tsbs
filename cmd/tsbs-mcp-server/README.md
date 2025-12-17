# TSBS MCP Server

基于 Model Context Protocol (MCP) Go SDK 实现的 TSBS 性能测试服务。


## 工具列表

### 执行工具

1. **tsbs_generate_data** - 生成 TSBS 测试数据
2. **tsbs_load_kwdb** - 将数据加载到 KWDB
3. **tsbs_generate_queries** - 生成测试查询
4. **tsbs_run_queries_kwdb** - 执行查询测试

### 状态查询工具

5. **get_tsbs_generate_data_status** - 查询数据生成状态
6. **get_tsbs_load_status** - 查询数据加载状态
7. **get_tsbs_generate_queries_status** - 查询查询生成状态
8. **get_tsbs_run_queries_status** - 查询查询执行状态

## 配置

### 配置文件

服务通过配置文件 `configs/config.yaml` 进行配置。配置文件示例：

```yaml
server:
  port: 8081          # HTTP 服务端口
  host: "0.0.0.0"     # 监听地址（0.0.0.0 表示所有网络接口）

# 任务状态数据库配置（用于存储任务状态和结果）
database:
  host: "localhost"   # 数据库主机地址
  port: 26257         # 数据库端口（KWDB 默认 26257，PostgreSQL 默认 5432）
  user: "root"        # 数据库用户名
  password: ""         # 数据库密码（建议使用环境变量 TSBS_DB_PASSWORD）
  dbname: "defaultdb"  # 数据库名称
  sslmode: "disable"  # SSL 模式（disable/require/verify-ca/verify-full）
  # SSL 证书配置（当 sslmode 为 verify-ca 或 verify-full 时需要）
  # sslcert: "/path/to/client.crt"      # 客户端证书文件路径（可选）
  # sslkey: "/path/to/client.key"       # 客户端密钥文件路径（可选）
  # sslrootcert: "/path/to/ca.crt"      # 根证书文件路径（verify-ca 和 verify-full 模式必需）

tsbs:
  bin_path: "./bin"                    # TSBS 二进制文件路径
  work_dir: "./tsbs_work"              # 工作目录
  data_dir: "./tsbs_work/load_data"    # 数据文件存储目录
  query_dir: "./tsbs_work/query_data"  # 查询文件存储目录
  reports_dir: "./tsbs_work/reports"    # 报告文件存储目录
```

### 环境变量

配置也可以通过环境变量覆盖，环境变量优先级高于配置文件：

- `TSBS_SERVER_PORT` - 服务端口（覆盖 `server.port`）
- `TSBS_SERVER_HOST` - 监听地址（覆盖 `server.host`）
- `TSBS_DB_HOST` - 数据库主机（覆盖 `database.host`）
- `TSBS_DB_PORT` - 数据库端口（覆盖 `database.port`）
- `TSBS_DB_USER` - 数据库用户（覆盖 `database.user`）
- `TSBS_DB_PASSWORD` - 数据库密码（覆盖 `database.password`）
- `TSBS_DB_NAME` - 数据库名（覆盖 `database.dbname`）
- `TSBS_DB_SSLMODE` - SSL 模式（覆盖 `database.sslmode`）
- `TSBS_DB_SSLCERT` - SSL 客户端证书文件路径（覆盖 `database.sslcert`）
- `TSBS_DB_SSLKEY` - SSL 客户端密钥文件路径（覆盖 `database.sslkey`）
- `TSBS_DB_SSLROOTCERT` - SSL 根证书文件路径（覆盖 `database.sslrootcert`）
- `TSBS_BIN_PATH` - TSBS 二进制文件路径（覆盖 `tsbs.bin_path`）
- `TSBS_WORK_DIR` - 工作目录（覆盖 `tsbs.work_dir`）
- `TSBS_DATA_DIR` - 数据文件目录（覆盖 `tsbs.data_dir`）
- `TSBS_QUERY_DIR` - 查询文件目录（覆盖 `tsbs.query_dir`）
- `TSBS_REPORTS_DIR` - 报告文件目录（覆盖 `tsbs.reports_dir`）

### 配置说明

#### 服务器配置

- `server.port`: HTTP 服务监听端口，默认 8081
- `server.host`: 监听地址，`0.0.0.0` 表示监听所有网络接口，`127.0.0.1` 表示仅本地访问

#### 数据库配置

数据库用于存储任务状态、进度和结果。支持 PostgreSQL 和 KWDB。

- `database.host`: 数据库主机地址
- `database.port`: 数据库端口
  - KWDB: 默认 26257
  - PostgreSQL: 默认 5432
- `database.user`: 数据库用户名
- `database.password`: 数据库密码
- `database.dbname`: 数据库名称
- `database.sslmode`: SSL 连接模式
  - `disable`: 禁用 SSL（开发环境）
  - `require`: 需要 SSL（不验证证书）
  - `verify-ca`: 验证 CA 证书（需要 `sslrootcert`）
  - `verify-full`: 完整验证（需要 `sslrootcert`，可选 `sslcert` 和 `sslkey`）
- `database.sslcert`: SSL 客户端证书文件路径（可选，用于客户端证书认证）
- `database.sslkey`: SSL 客户端密钥文件路径（可选，用于客户端证书认证）
- `database.sslrootcert`: SSL 根证书（CA）文件路径（`verify-ca` 和 `verify-full` 模式必需）

**SSL 证书配置说明**：

当 `sslmode` 设置为 `verify-ca` 或 `verify-full` 时，需要在配置文件中指定证书文件路径：

```yaml
database:
  sslmode: "verify-full"
  sslrootcert: "/path/to/ca.crt"      # 必需：用于验证服务器证书
  sslcert: "/path/to/client.crt"     # 可选：客户端证书（用于客户端证书认证）
  sslkey: "/path/to/client.key"      # 可选：客户端密钥（用于客户端证书认证）
```

- **verify-ca 模式**：需要配置 `sslrootcert` 来验证服务器证书
- **verify-full 模式**：需要配置 `sslrootcert` 来验证服务器证书，可选配置 `sslcert` 和 `sslkey` 进行客户端证书认证
- 证书文件路径可以是绝对路径或相对路径（相对于工作目录）
- 建议将证书文件放在安全的位置，并设置适当的文件权限（如 `chmod 600`）
- 也可以通过环境变量配置：`TSBS_DB_SSLROOTCERT`、`TSBS_DB_SSLCERT`、`TSBS_DB_SSLKEY`

#### TSBS 配置

- `tsbs.bin_path`: TSBS 二进制文件所在目录，需要包含以下文件：
  - `tsbs_generate_data`
  - `tsbs_load_kwdb`
  - `tsbs_generate_queries`
  - `tsbs_run_queries_kwdb`
- `tsbs.work_dir`: 工作目录根路径
- `tsbs.data_dir`: 生成的数据文件存储目录
- `tsbs.query_dir`: 生成的查询文件存储目录
- `tsbs.reports_dir`: 测试报告存储目录

### MCP 客户端配置

在 MCP 客户端（如 Cursor）中配置服务器：

#### Cursor 配置示例

编辑 `~/.cursor/mcp.json`：

```json
{
  "mcpServers": {
    "tsbs-mcp-server": {
      "url": "http://localhost:8081"
    }
  }
}
```


## 运行

### 构建

```bash
make mcp-server-build
```

或

```bash
go build -o bin/tsbs-mcp-server ./cmd/tsbs-mcp-server
```

### 启动服务

```bash
./bin/tsbs-mcp-server
```

服务将在配置的端口（默认 8081）上启动。

## API 使用示例

服务通过 HTTP 暴露 MCP JSON-RPC 接口。客户端需要发送 JSON-RPC 2.0 格式的请求。

### 生成数据示例

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "tools/call",
  "params": {
    "name": "tsbs_generate_data",
    "arguments": {
      "use_case": "cpu-only",
      "scale": 1000,
      "timestamp_start": "2016-01-01T00:00:00Z",
      "timestamp_end": "2016-02-01T00:00:00Z",
      "log_interval": "10s"
    }
  }
}
```

### 查询状态示例

```json
{
  "jsonrpc": "2.0",
  "id": 2,
  "method": "tools/call",
  "params": {
    "name": "get_tsbs_generate_data_status",
    "arguments": {
      "test_task_id": "任务ID"
    }
  }
}
```

## 数据库表结构

服务会自动创建以下表：

- `tsbs_test_tasks` - 主任务表
- `tsbs_test_subtasks` - 子任务表
- `tsbs_test_results` - 结果表

## 依赖

- Go 1.21+
- PostgreSQL 或 KWDB
- TSBS 二进制文件（tsbs_generate_data, tsbs_load_kwdb, tsbs_generate_queries, tsbs_run_queries_kwdb）

