package mcp_tools

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/timescale/tsbs/internal/config"
	"github.com/timescale/tsbs/internal/service"
	"github.com/timescale/tsbs/internal/utils"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
	"github.com/timescale/tsbs/pkg/targets/constants"
)

// RegisterTools 注册所有 MCP 工具
func RegisterTools(
	server *mcp.Server,
	taskService *service.TaskService,
	statusService *service.StatusService,
	executionService *service.ExecutionService,
	cfg *config.Config,
) {
	// 数据生成工具
	mcp.AddTool(server, &mcp.Tool{
		Name:        "tsbs_generate_data",
		Description: "Generate TSBS test data. Creates test data files based on specified use case, scale, and time range.",
		InputSchema: map[string]interface{}{
			"type": "object",
			"properties": map[string]interface{}{
				"use_case": map[string]interface{}{
					"type":        "string",
					"description": "Use case type. Supported options: cpu-only, cpu-single, devops, iot, devops-generic. Note: If format is 'kwdb', only 'cpu-only' and 'iot' are supported. Default: 'cpu-only'",
					"enum":        []string{"cpu-only", "cpu-single", "devops", "iot", "devops-generic"},
					"default":     "cpu-only",
				},
				"seed": map[string]interface{}{
					"type":        "integer",
					"description": "Random seed for generating reproducible data. Default: 123",
					"default":     123,
				},
				"scale": map[string]interface{}{
					"type":        "integer",
					"description": "Data scale (number of devices/hosts). Must be greater than 0. Examples: 1, 10, 100, 1000",
					"minimum":     1,
				},
				"log_interval": map[string]interface{}{
					"type":        "string",
					"description": "Logging interval. Format: number + unit (s=seconds, m=minutes, h=hours). Examples: '10s', '1m', '1h'. Default: '10s'",
					"default":     "10s",
					"pattern":     "^\\d+[smh]$",
				},
				"timestamp_start": map[string]interface{}{
					"type":        "string",
					"description": "Start timestamp in RFC3339 format. Examples: '2016-01-01T00:00:00Z', '2025-12-25T00:00:00Z'",
					"format":      "date-time",
				},
				"timestamp_end": map[string]interface{}{
					"type":        "string",
					"description": "End timestamp in RFC3339 format. Must be later than timestamp_start. Examples: '2016-01-01T01:00:00Z', '2025-12-25T08:00:00Z'",
					"format":      "date-time",
				},
				"format": map[string]interface{}{
					"type":        "string",
					"description": "Data format. Supported formats: cassandra, clickhouse, influx, mongo, siridb, timescaledb, akumuli, cratedb, prometheus, victoriametrics, timestream, questdb, kwdb. Default: 'kwdb'",
					"enum":        []string{"cassandra", "clickhouse", "influx", "mongo", "siridb", "timescaledb", "akumuli", "cratedb", "prometheus", "victoriametrics", "timestream", "questdb", "kwdb"},
					"default":     "kwdb",
				},
				"orderquantity": map[string]interface{}{
					"type":        "integer",
					"description": "Order quantity (optional). Only used for certain use cases.",
				},
				"outoforder": map[string]interface{}{
					"type":        "number",
					"description": "Out-of-order ratio, a float between 0.0-1.0 (optional). Only used for cpu-only use case.",
					"minimum":     0.0,
					"maximum":     1.0,
				},
				"outoforderwindow": map[string]interface{}{
					"type":        "string",
					"description": "Out-of-order time window (optional). Only used for cpu-only use case. Format: number + unit (s=seconds, m=minutes, h=hours).",
					"pattern":     "^\\d+[smh]$",
				},
				"output_file": map[string]interface{}{
					"type":        "string",
					"description": "Output file path (optional). If not specified, filename will be auto-generated: {use_case}_{format}_scale_{scale}_{order}order.dat",
				},
			},
			"required": []string{"scale", "timestamp_start", "timestamp_end"},
		},
	}, func(ctx context.Context, req *mcp.CallToolRequest, input GenerateDataInput) (*mcp.CallToolResult, GenerateDataOutput, error) {
		return handleGenerateData(ctx, req, input, taskService, executionService)
	})

	// 数据加载工具
	mcp.AddTool(server, &mcp.Tool{
		Name:        "tsbs_load_kwdb",
		Description: "Load generated test data into KWDB database. ",
		InputSchema: map[string]interface{}{
			"type": "object",
			"properties": map[string]interface{}{
				"file": map[string]interface{}{
					"type":        "string",
					"description": "Path to the data file to load. Example: 'tsbs_work/load_data/cpu-only_kwdb_scale_1_12order.dat'",
				},
				"insert_type": map[string]interface{}{
					"type":        "string",
					"description": "Insert type. Must be one of: 'insert' (regular insert), 'prepare' (prepared statement), 'prepareiot' (IoT prepared). Default: 'insert'",
					"enum":        []string{"insert", "prepare", "prepareiot"},
					"default":     "insert",
				},
				"case": map[string]interface{}{
					"type":        "string",
					"description": "Use case type. Must match the use_case used when generating data. Examples: 'cpu-only', 'iot'",
				},
				"batch_size": map[string]interface{}{
					"type":        "integer",
					"description": "Batch size (optional). Number of records per insert.",
				},
				"preparesize": map[string]interface{}{
					"type":        "integer",
					"description": "Prepared statement size (optional). Only used for prepare and prepareiot insert types.",
				},
				"workers": map[string]interface{}{
					"type":        "integer",
					"description": "Number of worker threads (optional). Number of concurrent threads for loading data.",
				},
				"partition": map[string]interface{}{
					"type":        "boolean",
					"description": "Enable partitioning (optional). Should be set to false for single-node clusters. Default: false",
					"default":     false,
				},
			},
			"required": []string{"file", "insert_type", "case"},
		},
	}, func(ctx context.Context, req *mcp.CallToolRequest, input LoadDataInput) (*mcp.CallToolResult, LoadDataOutput, error) {
		return handleLoadData(ctx, req, input, taskService, executionService, cfg)
	})

	// 查询生成工具
	mcp.AddTool(server, &mcp.Tool{
		Name:        "tsbs_generate_queries",
		Description: "Generate TSBS test queries. Creates query files based on specified use case, query type, and time range. Note: Time interval must be large enough to meet query type requirements (e.g., single-groupby-1-1-1 requires at least 1 hour).",
		InputSchema: map[string]interface{}{
			"type": "object",
			"properties": map[string]interface{}{
				"use_case": map[string]interface{}{
					"type":        "string",
					"description": "Use case type. Supported options: cpu-only, cpu-single, devops, iot, devops-generic. Note: If format is 'kwdb', only 'cpu-only' and 'iot' are supported. Default: 'cpu-only'",
					"enum":        []string{"cpu-only", "cpu-single", "devops", "iot", "devops-generic"},
					"default":     "cpu-only",
				},
				"seed": map[string]interface{}{
					"type":        "integer",
					"description": "Random seed for generating reproducible queries. Default: 123",
					"default":     123,
				},
				"scale": map[string]interface{}{
					"type":        "integer",
					"description": "Data scale (number of devices/hosts). Must match the scale used when generating data. Examples: 1, 10, 100, 1000",
					"minimum":     1,
				},
				"query_type": map[string]interface{}{
					"type":        "string",
					"description": "Query type. For cpu-only: single-groupby-1-1-1, single-groupby-1-1-12, single-groupby-1-8-1, single-groupby-5-1-1, single-groupby-5-1-12, single-groupby-5-8-1, cpu-max-all-1, cpu-max-all-8, double-groupby-1, double-groupby-5, double-groupby-all, high-cpu-1, high-cpu-all, lastpoint, groupby-orderby-limit. For iot: last-loc, single-last-loc, low-fuel, high-load, stationary-trucks, long-driving-sessions, long-daily-sessions, avg-vs-proj-fuel-consumption, avg-daily-driving-duration, avg-daily-driving-session, daily-activity, breakdown-frequency, avg-load",
				},
				"format": map[string]interface{}{
					"type":        "string",
					"description": "Data format. Supported formats: cassandra, clickhouse, influx, mongo, siridb, timescaledb, akumuli, cratedb, prometheus, victoriametrics, timestream, questdb, kwdb. Default: 'kwdb'",
					"enum":        []string{"cassandra", "clickhouse", "influx", "mongo", "siridb", "timescaledb", "akumuli", "cratedb", "prometheus", "victoriametrics", "timestream", "questdb", "kwdb"},
					"default":     "kwdb",
				},
				"queries": map[string]interface{}{
					"type":        "integer",
					"description": "Number of queries to generate. Examples: 10, 100, 1000",
					"minimum":     1,
				},
				"timestamp_start": map[string]interface{}{
					"type":        "string",
					"description": "Start timestamp in RFC3339 format. Must match or be within the range of timestamp_start used when generating data. Example: '2016-01-01T00:00:00Z'",
					"format":      "date-time",
				},
				"timestamp_end": map[string]interface{}{
					"type":        "string",
					"description": "End timestamp in RFC3339 format. Must be later than timestamp_start, and time interval must meet query type requirements. Example: '2016-01-01T08:00:00Z' (single-groupby-1-1-1 requires at least 1 hour)",
					"format":      "date-time",
				},
				"prepare": map[string]interface{}{
					"type":        "boolean",
					"description": "Generate prepared queries (optional). Default: false",
					"default":     false,
				},
				"output_file": map[string]interface{}{
					"type":        "string",
					"description": "Output file path (optional). If not specified, filename will be auto-generated.",
				},
			},
			"required": []string{"scale", "query_type", "queries", "timestamp_start", "timestamp_end"},
		},
	}, func(ctx context.Context, req *mcp.CallToolRequest, input GenerateQueriesInput) (*mcp.CallToolResult, GenerateQueriesOutput, error) {
		return handleGenerateQueries(ctx, req, input, taskService, executionService, cfg)
	})

	// 查询执行工具
	mcp.AddTool(server, &mcp.Tool{
		Name:        "tsbs_run_queries_kwdb",
		Description: "Execute TSBS generated queries and return performance metrics.",
		InputSchema: map[string]interface{}{
			"type": "object",
			"properties": map[string]interface{}{
				"file": map[string]interface{}{
					"type":        "string",
					"description": "Path to the query file to execute. Example: 'tsbs_work/query_data/scale1/kwdb_scale1_cpu-only_lastpoint_query_times1.dat'",
				},
				"workers": map[string]interface{}{
					"type":        "integer",
					"description": "Number of worker threads (optional). Number of concurrent threads for executing queries. Examples: 1, 8",
				},
				"prepare": map[string]interface{}{
					"type":        "boolean",
					"description": "Use prepared queries (optional). Must match the prepare parameter used when generating queries. Default: false",
					"default":     false,
				},
				"query_type": map[string]interface{}{
					"type":        "string",
					"description": "Query type. Must match the query_type used when generating queries. Examples: 'lastpoint', 'single-groupby-1-1-1', 'cpu-max-all-1'",
				},
				"server_config_name": map[string]interface{}{
					"type":        "string",
					"description": "Server configuration name (optional).",
				},
			},
			"required": []string{"file", "query_type"},
		},
	}, func(ctx context.Context, req *mcp.CallToolRequest, input RunQueriesInput) (*mcp.CallToolResult, RunQueriesOutput, error) {
		return handleRunQueries(ctx, req, input, taskService, executionService, cfg)
	})

	// 状态查询工具
	mcp.AddTool(server, &mcp.Tool{
		Name:        "get_tsbs_generate_data_status",
		Description: "Query the execution status and results of data generation task. Returns task status (running/completed/failed), progress percentage, output file path, etc.",
		InputSchema: map[string]interface{}{
			"type": "object",
			"properties": map[string]interface{}{
				"test_task_id": map[string]interface{}{
					"type":        "string",
					"description": "Task ID. Get the task_id from the return result of tsbs_generate_data tool.",
				},
			},
			"required": []string{"test_task_id"},
		},
		OutputSchema: map[string]interface{}{
			"type": "object",
			"properties": map[string]interface{}{
				"status": map[string]interface{}{
					"type":        "string",
					"description": "Task status: running, completed, failed, or error",
				},
				"progress": map[string]interface{}{
					"type":        "integer",
					"description": "Task progress percentage (0-100)",
				},
				"message": map[string]interface{}{
					"type":        "string",
					"description": "Status message or error message",
				},
				"output_file": map[string]interface{}{
					"type":        "string",
					"description": "Output file path",
				},
				"error": map[string]interface{}{
					"type":        "string",
					"description": "Error message if task failed",
				},
				"result": map[string]interface{}{
					"type":        "object",
					"description": "Task result data",
				},
				"metrics": map[string]interface{}{
					"type":        "object",
					"description": "Performance metrics",
				},
			},
		},
	}, func(ctx context.Context, req *mcp.CallToolRequest, input StatusInput) (*mcp.CallToolResult, StatusOutput, error) {
		return handleGetGenerateDataStatus(ctx, req, input, statusService)
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "get_tsbs_load_status",
		Description: "Query the execution status and results of data loading task. Returns task status (running/completed/failed), progress percentage, loading results, etc.",
		InputSchema: map[string]interface{}{
			"type": "object",
			"properties": map[string]interface{}{
				"test_task_id": map[string]interface{}{
					"type":        "string",
					"description": "Task ID. Get the task_id from the return result of tsbs_load_kwdb tool.",
				},
			},
			"required": []string{"test_task_id"},
		},
		OutputSchema: map[string]interface{}{
			"type": "object",
			"properties": map[string]interface{}{
				"status": map[string]interface{}{
					"type":        "string",
					"description": "Task status: running, completed, failed, or error",
				},
				"progress": map[string]interface{}{
					"type":        "integer",
					"description": "Task progress percentage (0-100)",
				},
				"message": map[string]interface{}{
					"type":        "string",
					"description": "Status message or error message",
				},
				"output_file": map[string]interface{}{
					"type":        "string",
					"description": "Output file path",
				},
				"error": map[string]interface{}{
					"type":        "string",
					"description": "Error message if task failed",
				},
				"result": map[string]interface{}{
					"type":        "object",
					"description": "Task result data",
				},
				"metrics": map[string]interface{}{
					"type":        "object",
					"description": "Performance metrics",
				},
			},
		},
	}, func(ctx context.Context, req *mcp.CallToolRequest, input StatusInput) (*mcp.CallToolResult, StatusOutput, error) {
		return handleGetLoadStatus(ctx, req, input, statusService)
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "get_tsbs_generate_queries_status",
		Description: "Query the execution status and results of query generation task. Returns task status (running/completed/failed), progress percentage, output file path, etc.",
		InputSchema: map[string]interface{}{
			"type": "object",
			"properties": map[string]interface{}{
				"test_task_id": map[string]interface{}{
					"type":        "string",
					"description": "Task ID. Get the task_id from the return result of tsbs_generate_queries tool.",
				},
			},
			"required": []string{"test_task_id"},
		},
		OutputSchema: map[string]interface{}{
			"type": "object",
			"properties": map[string]interface{}{
				"status": map[string]interface{}{
					"type":        "string",
					"description": "Task status: running, completed, failed, or error",
				},
				"progress": map[string]interface{}{
					"type":        "integer",
					"description": "Task progress percentage (0-100)",
				},
				"message": map[string]interface{}{
					"type":        "string",
					"description": "Status message or error message",
				},
				"output_file": map[string]interface{}{
					"type":        "string",
					"description": "Output file path",
				},
				"error": map[string]interface{}{
					"type":        "string",
					"description": "Error message if task failed",
				},
				"result": map[string]interface{}{
					"type":        "object",
					"description": "Task result data",
				},
				"metrics": map[string]interface{}{
					"type":        "object",
					"description": "Performance metrics",
				},
			},
		},
	}, func(ctx context.Context, req *mcp.CallToolRequest, input StatusInput) (*mcp.CallToolResult, StatusOutput, error) {
		return handleGetGenerateQueriesStatus(ctx, req, input, statusService)
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "get_tsbs_run_queries_status",
		Description: "Query the execution status and results of query execution task. Returns task status (running/completed/failed), progress percentage, performance metrics, etc. Can query status of main task or subtask.",
		InputSchema: map[string]interface{}{
			"type": "object",
			"properties": map[string]interface{}{
				"test_task_id": map[string]interface{}{
					"type":        "string",
					"description": "Task ID. Get the task_id from the return result of tsbs_run_queries_kwdb tool.",
				},
				"subtask_id": map[string]interface{}{
					"type":        "string",
					"description": "Subtask ID (optional). If provided, will query status of specific subtask; otherwise queries main task status.",
				},
			},
			"required": []string{"test_task_id"},
		},
		OutputSchema: map[string]interface{}{
			"type": "object",
			"properties": map[string]interface{}{
				"status": map[string]interface{}{
					"type":        "string",
					"description": "Task status: running, completed, failed, or error",
				},
				"progress": map[string]interface{}{
					"type":        "integer",
					"description": "Task progress percentage (0-100)",
				},
				"message": map[string]interface{}{
					"type":        "string",
					"description": "Status message or error message",
				},
				"output_file": map[string]interface{}{
					"type":        "string",
					"description": "Output file path",
				},
				"error": map[string]interface{}{
					"type":        "string",
					"description": "Error message if task failed",
				},
				"result": map[string]interface{}{
					"type":        "object",
					"description": "Task result data",
				},
				"metrics": map[string]interface{}{
					"type":        "object",
					"description": "Performance metrics",
				},
			},
		},
	}, func(ctx context.Context, req *mcp.CallToolRequest, input RunQueriesStatusInput) (*mcp.CallToolResult, StatusOutput, error) {
		return handleGetRunQueriesStatus(ctx, req, input, statusService)
	})
}

// 工具输入输出类型定义
type GenerateDataInput struct {
	UseCase          string   `json:"use_case"`
	Seed             int      `json:"seed"`
	Scale            int      `json:"scale"`
	LogInterval      string   `json:"log_interval"`
	TimestampStart   string   `json:"timestamp_start"`
	TimestampEnd     string   `json:"timestamp_end"`
	Format           string   `json:"format"`
	OrderQuantity    *int     `json:"orderquantity,omitempty"`
	OutOfOrder       *float64 `json:"outoforder,omitempty"`
	OutOfOrderWindow *string  `json:"outoforderwindow,omitempty"`
	OutputFile       *string  `json:"output_file,omitempty"`
}

type GenerateDataOutput struct {
	TaskID  string `json:"task_id"`
	Status  string `json:"status"`
	Message string `json:"message"`
}

type LoadDataInput struct {
	File        string `json:"file"`
	InsertType  string `json:"insert_type,omitempty"`
	Case        string `json:"case"`
	BatchSize   *int   `json:"batch_size,omitempty"`
	Preparesize *int   `json:"preparesize,omitempty"`
	Workers     *int   `json:"workers,omitempty"`
	Partition   *bool  `json:"partition,omitempty"`
}

type LoadDataOutput struct {
	TaskID  string `json:"task_id"`
	Status  string `json:"status"`
	Message string `json:"message"`
}

type GenerateQueriesInput struct {
	UseCase        string  `json:"use_case"`
	Seed           int     `json:"seed"`
	Scale          int     `json:"scale"`
	QueryType      string  `json:"query_type"`
	Format         string  `json:"format"`
	Queries        int     `json:"queries"`
	TimestampStart string  `json:"timestamp_start"`
	TimestampEnd   string  `json:"timestamp_end"`
	Prepare        *bool   `json:"prepare,omitempty"`
	OutputFile     *string `json:"output_file,omitempty"`
}

type GenerateQueriesOutput struct {
	TaskID  string `json:"task_id"`
	Status  string `json:"status"`
	Message string `json:"message"`
}

type RunQueriesInput struct {
	File             string  `json:"file"`
	Workers          *int    `json:"workers,omitempty"`
	Prepare          *bool   `json:"prepare,omitempty"`
	QueryType        string  `json:"query_type"`
	ServerConfigName *string `json:"server_config_name,omitempty"`
}

type RunQueriesOutput struct {
	TaskID  string `json:"task_id"`
	Status  string `json:"status"`
	Message string `json:"message"`
}

type StatusInput struct {
	TestTaskID string `json:"test_task_id"`
}

type RunQueriesStatusInput struct {
	TestTaskID string  `json:"test_task_id"`
	SubtaskID  *string `json:"subtask_id,omitempty"`
}

type StatusOutput struct {
	Status     string          `json:"status"`
	Progress   int             `json:"progress"`
	Message    string          `json:"message,omitempty"`
	OutputFile string          `json:"output_file,omitempty"`
	Error      string          `json:"error,omitempty"`
	Result     json.RawMessage `json:"result,omitempty"`
	Metrics    json.RawMessage `json:"metrics,omitempty"`
}

// 工具处理函数
func handleGenerateData(
	ctx context.Context,
	_ *mcp.CallToolRequest,
	input GenerateDataInput,
	taskService *service.TaskService,
	executionService *service.ExecutionService,
) (*mcp.CallToolResult, GenerateDataOutput, error) {
	// 参数验证和默认值（参考 scripts/tsbs_kwdb.sh）
	if input.Format == "" {
		input.Format = "kwdb"
	}
	if input.LogInterval == "" {
		input.LogInterval = "10s"
	}
	// seed 默认值 123（脚本中固定使用 123）
	if input.Seed == 0 {
		input.Seed = 123
	}

	// 参数验证：在创建任务之前验证参数有效性
	// 参数验证错误返回正常的 output，不返回 error，让 LLM 能够读取错误并调整参数
	// 验证 format
	if !utils.IsIn(input.Format, constants.SupportedFormats()) {
		return nil, GenerateDataOutput{
			TaskID:  "",
			Status:  "error",
			Message: fmt.Sprintf("参数验证失败: invalid format: %s, supported formats: %s", input.Format, strings.Join(constants.SupportedFormats(), ", ")),
		}, nil
	}

	// 验证 use_case
	if input.UseCase == "" {
		input.UseCase = common.UseCaseCPUOnly
	}
	if !utils.IsIn(input.UseCase, common.UseCaseChoices) {
		return nil, GenerateDataOutput{
			TaskID:  "",
			Status:  "error",
			Message: fmt.Sprintf("参数验证失败: invalid use_case: %s, supported use cases: %s", input.UseCase, strings.Join(common.UseCaseChoices, ", ")),
		}, nil
	}

	// 如果 format 是 kwdb，只支持 cpu-only 和 iot
	if input.Format == "kwdb" {
		kwdbSupportedUseCases := []string{common.UseCaseCPUOnly, common.UseCaseIoT}
		if !utils.IsIn(input.UseCase, kwdbSupportedUseCases) {
			return nil, GenerateDataOutput{
				TaskID:  "",
				Status:  "error",
				Message: fmt.Sprintf("参数验证失败: invalid use_case for kwdb format: %s, kwdb only supports: %s", input.UseCase, strings.Join(kwdbSupportedUseCases, ", ")),
			}, nil
		}
	}

	// 验证 scale
	if input.Scale <= 0 {
		return nil, GenerateDataOutput{
			TaskID:  "",
			Status:  "error",
			Message: fmt.Sprintf("参数验证失败: scale must be greater than 0, got: %d", input.Scale),
		}, nil
	}

	// 验证时间戳格式
	if input.TimestampStart == "" {
		return nil, GenerateDataOutput{
			TaskID:  "",
			Status:  "error",
			Message: "参数验证失败: timestamp_start is required",
		}, nil
	}
	if _, err := time.Parse(time.RFC3339, input.TimestampStart); err != nil {
		return nil, GenerateDataOutput{
			TaskID:  "",
			Status:  "error",
			Message: fmt.Sprintf("参数验证失败: invalid timestamp_start format: %v, expected RFC3339 format (e.g., 2016-01-01T00:00:00Z)", err),
		}, nil
	}

	if input.TimestampEnd == "" {
		return nil, GenerateDataOutput{
			TaskID:  "",
			Status:  "error",
			Message: "参数验证失败: timestamp_end is required",
		}, nil
	}
	if _, err := time.Parse(time.RFC3339, input.TimestampEnd); err != nil {
		return nil, GenerateDataOutput{
			TaskID:  "",
			Status:  "error",
			Message: fmt.Sprintf("参数验证失败: invalid timestamp_end format: %v, expected RFC3339 format (e.g., 2016-01-01T00:00:00Z)", err),
		}, nil
	}

	// 验证时间范围
	startTime, _ := time.Parse(time.RFC3339, input.TimestampStart)
	endTime, _ := time.Parse(time.RFC3339, input.TimestampEnd)
	if endTime.Before(startTime) || endTime.Equal(startTime) {
		return nil, GenerateDataOutput{
			TaskID:  "",
			Status:  "error",
			Message: "参数验证失败: timestamp_end must be after timestamp_start",
		}, nil
	}

	// 验证 log_interval 格式
	if _, err := time.ParseDuration(input.LogInterval); err != nil {
		return nil, GenerateDataOutput{
			TaskID:  "",
			Status:  "error",
			Message: fmt.Sprintf("参数验证失败: invalid log_interval format: %v, expected duration format (e.g., 10s, 1m, 1h)", err),
		}, nil
	}

	taskID, err := taskService.CreateTask(ctx, "generate_data", input)
	if err != nil {
		return nil, GenerateDataOutput{
			TaskID:  "",
			Status:  "error",
			Message: fmt.Sprintf("任务创建失败: %v", err),
		}, nil
	}

	// 转换为 service 包的类型
	serviceInput := service.GenerateDataInput{
		UseCase:          input.UseCase,
		Seed:             input.Seed,
		Scale:            input.Scale,
		LogInterval:      input.LogInterval,
		TimestampStart:   input.TimestampStart,
		TimestampEnd:     input.TimestampEnd,
		Format:           input.Format,
		OrderQuantity:    input.OrderQuantity,
		OutOfOrder:       input.OutOfOrder,
		OutOfOrderWindow: input.OutOfOrderWindow,
		OutputFile:       input.OutputFile,
	}
	// 异步执行
	go executionService.ExecuteGenerateData(ctx, taskID, serviceInput)

	return nil, GenerateDataOutput{
		TaskID:  taskID,
		Status:  "running",
		Message: "数据生成任务已启动",
	}, nil
}

func handleLoadData(
	ctx context.Context,
	_ *mcp.CallToolRequest,
	input LoadDataInput,
	taskService *service.TaskService,
	executionService *service.ExecutionService,
	cfg *config.Config,
) (*mcp.CallToolResult, LoadDataOutput, error) {
	// 从 config 读取默认值，如果用户未提供则使用 config 中的值
	host := cfg.Database.Host
	port := cfg.Database.Port
	user := cfg.Database.User
	password := cfg.Database.Password
	testDBName := cfg.TSBS.TestDBName
	if testDBName == "" {
		testDBName = "tsbs" // 默认值
	}
	dbName := testDBName
	if input.InsertType == "" {
		input.InsertType = "insert"
	} else {
		validTypes := map[string]bool{
			"insert":     true,
			"prepare":    true,
			"prepareiot": true,
		}
		if !validTypes[input.InsertType] {
			return nil, LoadDataOutput{
				TaskID:  "",
				Status:  "error",
				Message: fmt.Sprintf("参数验证失败: invalid insert_type: %s, must be one of: insert, prepare, prepareiot", input.InsertType),
			}, nil
		}
	}
	if input.Partition == nil {
		partition := false
		input.Partition = &partition
	}

	taskID, err := taskService.CreateTask(ctx, "load", input)
	if err != nil {
		return nil, LoadDataOutput{
			TaskID:  "",
			Status:  "error",
			Message: fmt.Sprintf("任务创建失败: %v", err),
		}, nil
	}

	serviceInput := service.LoadDataInput{
		File:        input.File,
		User:        user,
		Password:    password,
		Host:        host,
		Port:        port,
		InsertType:  input.InsertType,
		DBName:      dbName,
		Case:        input.Case,
		BatchSize:   input.BatchSize,
		Preparesize: input.Preparesize,
		Workers:     input.Workers,
		Partition:   input.Partition,
	}
	go executionService.ExecuteLoadData(ctx, taskID, serviceInput)

	return nil, LoadDataOutput{
		TaskID:  taskID,
		Status:  "running",
		Message: "数据加载任务已启动",
	}, nil
}

func handleGenerateQueries(
	ctx context.Context,
	_ *mcp.CallToolRequest,
	input GenerateQueriesInput,
	taskService *service.TaskService,
	executionService *service.ExecutionService,
	cfg *config.Config,
) (*mcp.CallToolResult, GenerateQueriesOutput, error) {
	// 参数验证和默认值（参考 scripts/tsbs_kwdb.sh）
	if input.Format == "" {
		input.Format = "kwdb"
	}
	// seed 默认值 123（脚本中固定使用 123）
	if input.Seed == 0 {
		input.Seed = 123
	}
	// prepare 默认 false（脚本中默认 false）
	if input.Prepare == nil {
		prepare := false
		input.Prepare = &prepare
	}

	// 验证 use_case（如果 format 是 kwdb，只支持 cpu-only 和 iot）
	if input.UseCase == "" {
		input.UseCase = common.UseCaseCPUOnly
	}
	if input.Format == "kwdb" {
		kwdbSupportedUseCases := []string{common.UseCaseCPUOnly, common.UseCaseIoT}
		if !utils.IsIn(input.UseCase, kwdbSupportedUseCases) {
			return nil, GenerateQueriesOutput{
				TaskID:  "",
				Status:  "error",
				Message: fmt.Sprintf("参数验证失败: invalid use_case for kwdb format: %s, kwdb only supports: %s", input.UseCase, strings.Join(kwdbSupportedUseCases, ", ")),
			}, nil
		}
	}

	// 验证时间间隔是否足够大（根据查询类型）
	// 解析时间戳
	startTime, err := time.Parse(time.RFC3339, input.TimestampStart)
	if err != nil {
		return nil, GenerateQueriesOutput{
			TaskID:  "",
			Status:  "error",
			Message: fmt.Sprintf("参数验证失败: invalid timestamp-start format: %v, expected RFC3339 format (e.g., 2016-01-01T00:00:00Z)", err),
		}, nil
	}
	endTime, err := time.Parse(time.RFC3339, input.TimestampEnd)
	if err != nil {
		return nil, GenerateQueriesOutput{
			TaskID:  "",
			Status:  "error",
			Message: fmt.Sprintf("参数验证失败: invalid timestamp-end format: %v, expected RFC3339 format (e.g., 2016-01-01T00:00:00Z)", err),
		}, nil
	}
	timeInterval := endTime.Sub(startTime)

	// 根据查询类型确定最小时间间隔要求
	var minInterval time.Duration
	if input.UseCase == "cpu-only" {
		// 根据查询类型确定最小时间间隔
		if strings.HasPrefix(input.QueryType, "single-groupby") {
			// single-groupby-X-X-X 中的最后一个数字是小时数
			// 例如 single-groupby-1-1-1 需要 1 小时
			parts := strings.Split(input.QueryType, "-")
			if len(parts) >= 4 {
				var hours int
				if _, parseErr := fmt.Sscanf(parts[3], "%d", &hours); parseErr == nil && hours > 0 {
					minInterval = time.Duration(hours) * time.Hour
				} else {
					minInterval = time.Hour // 默认 1 小时
				}
			} else {
				minInterval = time.Hour // 默认 1 小时
			}
		} else if strings.HasPrefix(input.QueryType, "double-groupby") {
			minInterval = 12 * time.Hour
		} else if strings.HasPrefix(input.QueryType, "high-cpu") {
			minInterval = 12 * time.Hour
		} else if strings.HasPrefix(input.QueryType, "cpu-max-all") {
			minInterval = 8 * time.Hour
		} else {
			// 其他查询类型默认需要 1 小时
			minInterval = time.Hour
		}
	} else {
		// IoT 用例也需要一定的时间间隔
		minInterval = time.Hour
	}

	// 检查时间间隔是否足够大
	if timeInterval < minInterval {
		return nil, GenerateQueriesOutput{
			TaskID: "",
			Status: "error",
			Message: fmt.Sprintf("参数验证失败: time interval too small: got %v, need at least %v for query type %s. Please increase the time range between timestamp-start and timestamp-end",
				timeInterval, minInterval, input.QueryType),
		}, nil
	}

	taskID, err := taskService.CreateTask(ctx, "generate_queries", input)
	if err != nil {
		return nil, GenerateQueriesOutput{
			TaskID:  "",
			Status:  "error",
			Message: fmt.Sprintf("任务创建失败: %v", err),
		}, nil
	}

	testDBName := cfg.TSBS.TestDBName
	if testDBName == "" {
		testDBName = "tsbs" // 默认值
	}

	serviceInput := service.GenerateQueriesInput{
		UseCase:        input.UseCase,
		Seed:           input.Seed,
		Scale:          input.Scale,
		QueryType:      input.QueryType,
		Format:         input.Format,
		Queries:        input.Queries,
		DBName:         testDBName,
		TimestampStart: input.TimestampStart,
		TimestampEnd:   input.TimestampEnd,
		Prepare:        input.Prepare,
		OutputFile:     input.OutputFile,
	}
	go executionService.ExecuteGenerateQueries(ctx, taskID, serviceInput)

	return nil, GenerateQueriesOutput{
		TaskID:  taskID,
		Status:  "running",
		Message: "查询生成任务已启动",
	}, nil
}

func handleRunQueries(
	ctx context.Context,
	_ *mcp.CallToolRequest,
	input RunQueriesInput,
	taskService *service.TaskService,
	executionService *service.ExecutionService,
	cfg *config.Config,
) (*mcp.CallToolResult, RunQueriesOutput, error) {
	// 从 config 读取默认值，如果用户未提供则使用 config 中的值
	host := cfg.Database.Host
	port := cfg.Database.Port
	user := cfg.Database.User
	password := cfg.Database.Password
	// workers 默认值 1（脚本中先执行 worker 1，再执行 worker 8）
	if input.Workers == nil {
		workers := 1
		input.Workers = &workers
	}
	// prepare 默认 false（需与生成时一致）
	if input.Prepare == nil {
		prepare := false
		input.Prepare = &prepare
	}

	taskID, err := taskService.CreateTask(ctx, "run_queries", input)
	if err != nil {
		return nil, RunQueriesOutput{
			TaskID:  "",
			Status:  "error",
			Message: fmt.Sprintf("任务创建失败: %v", err),
		}, nil
	}

	// 转换为 service 包的类型
	serviceInput := service.RunQueriesInput{
		File:             input.File,
		User:             user,
		Password:         password,
		Host:             host,
		Port:             port,
		Workers:          input.Workers,
		Prepare:          input.Prepare,
		QueryType:        input.QueryType,
		ServerConfigName: input.ServerConfigName,
	}
	// 异步执行
	go executionService.ExecuteRunQueries(ctx, taskID, serviceInput)

	return nil, RunQueriesOutput{
		TaskID:  taskID,
		Status:  "running",
		Message: "查询执行任务已启动",
	}, nil
}

func handleGetGenerateDataStatus(
	ctx context.Context,
	_ *mcp.CallToolRequest,
	input StatusInput,
	statusService *service.StatusService,
) (*mcp.CallToolResult, StatusOutput, error) {
	status, err := statusService.GetTaskStatus(ctx, input.TestTaskID)
	if err != nil {
		return nil, StatusOutput{
			Status:   "error",
			Progress: 0,
			Message:  fmt.Sprintf("Failed to query task status: %v", err),
		}, nil
	}

	output := StatusOutput{
		Status:     status.Status,
		Progress:   status.Progress,
		Message:    status.Message,
		OutputFile: status.OutputFile,
		Error:      status.Error,
		Result:     json.RawMessage(status.Result),
		Metrics:    json.RawMessage(status.Metrics),
	}

	return nil, output, nil
}

func handleGetLoadStatus(
	ctx context.Context,
	_ *mcp.CallToolRequest,
	input StatusInput,
	statusService *service.StatusService,
) (*mcp.CallToolResult, StatusOutput, error) {
	status, err := statusService.GetTaskStatus(ctx, input.TestTaskID)
	if err != nil {
		return nil, StatusOutput{
			Status:   "error",
			Progress: 0,
			Message:  fmt.Sprintf("Failed to query task status: %v", err),
		}, nil
	}

	output := StatusOutput{
		Status:     status.Status,
		Progress:   status.Progress,
		Message:    status.Message,
		OutputFile: status.OutputFile,
		Error:      status.Error,
		Result:     json.RawMessage(status.Result),
		Metrics:    json.RawMessage(status.Metrics),
	}

	return nil, output, nil
}

func handleGetGenerateQueriesStatus(
	ctx context.Context,
	_ *mcp.CallToolRequest,
	input StatusInput,
	statusService *service.StatusService,
) (*mcp.CallToolResult, StatusOutput, error) {
	status, err := statusService.GetTaskStatus(ctx, input.TestTaskID)
	if err != nil {
		return nil, StatusOutput{
			Status:   "error",
			Progress: 0,
			Message:  fmt.Sprintf("Failed to query task status: %v", err),
		}, nil
	}

	output := StatusOutput{
		Status:     status.Status,
		Progress:   status.Progress,
		Message:    status.Message,
		OutputFile: status.OutputFile,
		Error:      status.Error,
		Result:     json.RawMessage(status.Result),
		Metrics:    json.RawMessage(status.Metrics),
	}

	return nil, output, nil
}

func handleGetRunQueriesStatus(
	ctx context.Context,
	_ *mcp.CallToolRequest,
	input RunQueriesStatusInput,
	statusService *service.StatusService,
) (*mcp.CallToolResult, StatusOutput, error) {
	var serviceStatus service.StatusOutput
	var err error

	if input.SubtaskID != nil {
		serviceStatus, err = statusService.GetSubtaskStatus(ctx, input.TestTaskID, *input.SubtaskID)
	} else {
		serviceStatus, err = statusService.GetTaskStatus(ctx, input.TestTaskID)
	}

	if err != nil {
		return nil, StatusOutput{
			Status:   "error",
			Progress: 0,
			Message:  fmt.Sprintf("Failed to query task status: %v", err),
		}, nil
	}

	output := StatusOutput{
		Status:     serviceStatus.Status,
		Progress:   serviceStatus.Progress,
		Message:    serviceStatus.Message,
		OutputFile: serviceStatus.OutputFile,
		Error:      serviceStatus.Error,
		Result:     json.RawMessage(serviceStatus.Result),
		Metrics:    json.RawMessage(serviceStatus.Metrics),
	}

	// 截断函数：截断过长的字符串输出
	truncateOutput := func(outputStr string) string {
		if len(outputStr) > 1000 {
			// 截断过长的输出，保留前 50 字符和后 200 字符，中间用省略号
			return outputStr[:50] + "\n... (truncated, " + fmt.Sprintf("%d", len(outputStr)-250) + " characters omitted) ...\n" + outputStr[len(outputStr)-200:]
		}
		return outputStr
	}

	// 截断 result 字段中的 metrics.output，并删除重复的 result.output
	if len(output.Result) > 0 {
		var resultValue map[string]interface{}
		if err := json.Unmarshal(output.Result, &resultValue); err == nil {
			// 删除重复的 result.output 字段（与 result.metrics.output 重复）
			delete(resultValue, "output")
			// 截断 result.metrics.output
			if metricsValue, ok := resultValue["metrics"].(map[string]interface{}); ok {
				if metricsOutputStr, ok := metricsValue["output"].(string); ok {
					metricsValue["output"] = truncateOutput(metricsOutputStr)
					resultValue["metrics"] = metricsValue
				}
			}
			if truncatedBytes, err := json.Marshal(resultValue); err == nil {
				output.Result = json.RawMessage(truncatedBytes)
			}
		}
	}

	// 截断 metrics.output 字段
	if len(output.Metrics) > 0 {
		var metricsValue map[string]interface{}
		if err := json.Unmarshal(output.Metrics, &metricsValue); err == nil {
			if outputStr, ok := metricsValue["output"].(string); ok {
				metricsValue["output"] = truncateOutput(outputStr)
				if truncatedBytes, err := json.Marshal(metricsValue); err == nil {
					output.Metrics = json.RawMessage(truncatedBytes)
				}
			}
		}
	}

	return nil, output, nil
}
