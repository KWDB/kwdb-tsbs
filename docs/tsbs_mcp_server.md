# TSBS MCP Server

The TSBS MCP Server is a Model Context Protocol (MCP) service implementation based on the Go SDK that provides an HTTP interface for TSBS performance testing tools. It enables AI assistants and other MCP clients to interact with TSBS tools through a standardized protocol.

This document is a supplement to the main README, detailing the following:

* MCP Server installation and configuration
* Available MCP tools for TSBS operations
* Configuration options and environment variables
* API usage examples

**This should be read *after* the main README [(supplemental docs)](../README.md).**

## Overview

The TSBS MCP Server exposes TSBS tools as MCP tools that can be called by AI assistants or other MCP clients. It provides:

- Asynchronous task execution with status tracking
- Database-backed task state management
- Streamable HTTP transport for MCP protocol
- Support for all TSBS operations: data generation, loading, query generation, and execution

## Tools

### Execution Tools

1. **tsbs_generate_data** - Generate TSBS test data
2. **tsbs_load_kwdb** - Load data into KWDB
3. **tsbs_generate_queries** - Generate test queries
4. **tsbs_run_queries_kwdb** - Execute query tests

### Status Query Tools

5. **get_tsbs_generate_data_status** - Query data generation status
6. **get_tsbs_load_status** - Query data loading status
7. **get_tsbs_generate_queries_status** - Query query generation status
8. **get_tsbs_run_queries_status** - Query query execution status

## Configuration

### Configuration File

The server is configured via `configs/config.yaml`. Example configuration:

```yaml
server:
  port: 8081          # HTTP service port
  host: "0.0.0.0"     # Listen address (0.0.0.0 means all network interfaces)

# Task state database configuration (for storing task status and results)
database:
  host: "localhost"   # Database host address
  port: 26257         # Database port (KWDB default 26257, PostgreSQL default 5432)
  user: "root"        # Database username
  password: ""         # Database password (recommended to use environment variable TSBS_DB_PASSWORD)
  dbname: "defaultdb"  # Metadata database name (for storing task status and results)
  sslmode: "disable"  # SSL mode (disable/require/verify-ca/verify-full)
  # SSL certificate configuration (required when sslmode is verify-ca or verify-full)
  # sslcert: "/path/to/client.crt"      # Client certificate file path (optional)
  # sslkey: "/path/to/client.key"       # Client key file path (optional)
  # sslrootcert: "/path/to/ca.crt"      # Root certificate file path (required for verify-ca and verify-full)

tsbs:
  bin_path: "./bin"                    # TSBS binary file path
  work_dir: "./tsbs_work"              # Work directory
  data_dir: "./tsbs_work/load_data"    # Data file storage directory
  query_dir: "./tsbs_work/query_data"  # Query file storage directory
  reports_dir: "./tsbs_work/reports"    # Report file storage directory
  test_dbname: "tsbs"                  # TSBS test database name
```

### Environment Variables

Configuration can also be overridden via environment variables, which take precedence over the configuration file:

- `TSBS_SERVER_PORT` - Service port (overrides `server.port`)
- `TSBS_SERVER_HOST` - Listen address (overrides `server.host`)
- `TSBS_DB_HOST` - Database host (overrides `database.host`)
- `TSBS_DB_PORT` - Database port (overrides `database.port`)
- `TSBS_DB_USER` - Database user (overrides `database.user`)
- `TSBS_DB_PASSWORD` - Database password (overrides `database.password`)
- `TSBS_DB_NAME` - Database name (overrides `database.dbname`)
- `TSBS_DB_SSLMODE` - SSL mode (overrides `database.sslmode`)
- `TSBS_DB_SSLCERT` - SSL client certificate file path (overrides `database.sslcert`)
- `TSBS_DB_SSLKEY` - SSL client key file path (overrides `database.sslkey`)
- `TSBS_DB_SSLROOTCERT` - SSL root certificate file path (overrides `database.sslrootcert`)
- `TSBS_BIN_PATH` - TSBS binary file path (overrides `tsbs.bin_path`)
- `TSBS_WORK_DIR` - Work directory (overrides `tsbs.work_dir`)
- `TSBS_DATA_DIR` - Data file directory (overrides `tsbs.data_dir`)
- `TSBS_QUERY_DIR` - Query file directory (overrides `tsbs.query_dir`)
- `TSBS_REPORTS_DIR` - Report file directory (overrides `tsbs.reports_dir`)

### Configuration Details

#### Server Configuration

- `server.port`: HTTP service listening port, default 8081
- `server.host`: Listen address, `0.0.0.0` means listen on all network interfaces, `127.0.0.1` means local access only

#### Database Configuration

**Metadata Database**: Used to store task status, progress, and results. Supports PostgreSQL and KWDB.

- `database.host`: Database host address
- `database.port`: Database port
  - KWDB: default 26257
  - PostgreSQL: default 5432
- `database.user`: Database username
- `database.password`: Database password
- `database.dbname`: Metadata database name (e.g., `tsbs_meta`), used to store task status and results
- `database.sslmode`: SSL connection mode
  - `disable`: Disable SSL (development environment)
  - `require`: Require SSL (no certificate verification)
  - `verify-ca`: Verify CA certificate (requires `sslrootcert`)
  - `verify-full`: Full verification (requires `sslrootcert`, optional `sslcert` and `sslkey`)
- `database.sslcert`: SSL client certificate file path (optional, for client certificate authentication)
- `database.sslkey`: SSL client key file path (optional, for client certificate authentication)
- `database.sslrootcert`: SSL root certificate (CA) file path (required for `verify-ca` and `verify-full` modes)

**SSL Certificate Configuration**:

When `sslmode` is set to `verify-ca` or `verify-full`, you need to specify certificate file paths in the configuration file:

```yaml
database:
  sslmode: "verify-full"
  sslrootcert: "/path/to/ca.crt"      # Required: for verifying server certificate
  sslcert: "/path/to/client.crt"     # Optional: client certificate (for client certificate authentication)
  sslkey: "/path/to/client.key"      # Optional: client key (for client certificate authentication)
```

- **verify-ca mode**: Requires `sslrootcert` to verify server certificate
- **verify-full mode**: Requires `sslrootcert` to verify server certificate, optionally configure `sslcert` and `sslkey` for client certificate authentication
- Certificate file paths can be absolute or relative (relative to working directory)
- It is recommended to place certificate files in a secure location and set appropriate file permissions (e.g., `chmod 600`)
- Can also be configured via environment variables: `TSBS_DB_SSLROOTCERT`, `TSBS_DB_SSLCERT`, `TSBS_DB_SSLKEY`

#### TSBS Configuration

- `tsbs.bin_path`: Directory containing TSBS binary files, must include:
  - `tsbs_generate_data`
  - `tsbs_load_kwdb`
  - `tsbs_generate_queries`
  - `tsbs_run_queries_kwdb`
- `tsbs.work_dir`: Root work directory path
- `tsbs.data_dir`: Generated data file storage directory
- `tsbs.query_dir`: Generated query file storage directory
- `tsbs.reports_dir`: Test report storage directory
- `tsbs.test_dbname`: TSBS test database name (default `"tsbs"`)

### MCP Client Configuration

Configure the server in MCP clients (e.g., Cursor):

#### Cursor Configuration Example

Edit `~/.cursor/mcp.json`:

```json
{
  "mcpServers": {
    "tsbs-mcp-server": {
      "url": "http://localhost:8081"
    }
  }
}
```

## Installation and Running

### Building

```bash
make mcp-server-build
```

Or:

```bash
go build -o bin/tsbs-mcp-server ./cmd/tsbs-mcp-server
```

### Starting the Service

```bash
./bin/tsbs-mcp-server
```

The service will start on the configured port (default 8081).

## API Usage Examples

The service exposes MCP JSON-RPC interface via HTTP. Clients need to send JSON-RPC 2.0 formatted requests.

### Generate Data Example

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

### Query Status Example

```json
{
  "jsonrpc": "2.0",
  "id": 2,
  "method": "tools/call",
  "params": {
    "name": "get_tsbs_generate_data_status",
    "arguments": {
      "test_task_id": "task-id-here"
    }
  }
}
```

## Database Schema

The service automatically creates the following tables:

- `tsbs_test_tasks` - Main task table
- `tsbs_test_subtasks` - Subtask table
- `tsbs_test_results` - Results table

## Dependencies

- Go 1.21+
- PostgreSQL or KWDB
- TSBS binary files (tsbs_generate_data, tsbs_load_kwdb, tsbs_generate_queries, tsbs_run_queries_kwdb)

