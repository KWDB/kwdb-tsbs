package mcp_tools

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/timescale/tsbs/internal/service"
)

// RegisterTools 注册所有 MCP 工具
func RegisterTools(
	server *mcp.Server,
	taskService *service.TaskService,
	statusService *service.StatusService,
	executionService *service.ExecutionService,
) {
	// 数据生成工具
	mcp.AddTool(server, &mcp.Tool{
		Name:        "tsbs_generate_data",
		Description: "生成TSBS测试数据",
	}, func(ctx context.Context, req *mcp.CallToolRequest, input GenerateDataInput) (*mcp.CallToolResult, GenerateDataOutput, error) {
		return handleGenerateData(ctx, req, input, taskService, executionService)
	})

	// 数据加载工具
	mcp.AddTool(server, &mcp.Tool{
		Name:        "tsbs_load_kwdb",
		Description: "将生成的测试数据加载到KWDB数据库",
	}, func(ctx context.Context, req *mcp.CallToolRequest, input LoadDataInput) (*mcp.CallToolResult, LoadDataOutput, error) {
		return handleLoadData(ctx, req, input, taskService, executionService)
	})

	// 查询生成工具
	mcp.AddTool(server, &mcp.Tool{
		Name:        "tsbs_generate_queries",
		Description: "生成TSBS测试查询",
	}, func(ctx context.Context, req *mcp.CallToolRequest, input GenerateQueriesInput) (*mcp.CallToolResult, GenerateQueriesOutput, error) {
		return handleGenerateQueries(ctx, req, input, taskService, executionService)
	})

	// 查询执行工具
	mcp.AddTool(server, &mcp.Tool{
		Name:        "tsbs_run_queries_kwdb",
		Description: "执行TSBS生成的查询并返回性能指标",
	}, func(ctx context.Context, req *mcp.CallToolRequest, input RunQueriesInput) (*mcp.CallToolResult, RunQueriesOutput, error) {
		return handleRunQueries(ctx, req, input, taskService, executionService)
	})

	// 状态查询工具
	mcp.AddTool(server, &mcp.Tool{
		Name:        "get_tsbs_generate_data_status",
		Description: "查询数据生成任务的执行状态和结果",
	}, func(ctx context.Context, req *mcp.CallToolRequest, input StatusInput) (*mcp.CallToolResult, StatusOutput, error) {
		return handleGetGenerateDataStatus(ctx, req, input, statusService)
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "get_tsbs_load_status",
		Description: "查询数据加载任务的执行状态和结果",
	}, func(ctx context.Context, req *mcp.CallToolRequest, input StatusInput) (*mcp.CallToolResult, StatusOutput, error) {
		return handleGetLoadStatus(ctx, req, input, statusService)
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "get_tsbs_generate_queries_status",
		Description: "查询查询生成任务的执行状态和结果",
	}, func(ctx context.Context, req *mcp.CallToolRequest, input StatusInput) (*mcp.CallToolResult, StatusOutput, error) {
		return handleGetGenerateQueriesStatus(ctx, req, input, statusService)
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "get_tsbs_run_queries_status",
		Description: "查询查询执行任务的执行状态和结果",
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
	User        string `json:"user"`
	Password    string `json:"password"`
	Host        string `json:"host"`
	Port        int    `json:"port"`
	InsertType  string `json:"insert_type"`
	DBName      string `json:"db_name"`
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
	DBName         string  `json:"db_name"`
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
	User             string  `json:"user"`
	Password         string  `json:"password"`
	Host             string  `json:"host"`
	Port             int     `json:"port"`
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
	req *mcp.CallToolRequest,
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

	taskID, err := taskService.CreateTask(ctx, "generate_data", input)
	if err != nil {
		return &mcp.CallToolResult{IsError: true}, GenerateDataOutput{}, err
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
	req *mcp.CallToolRequest,
	input LoadDataInput,
	taskService *service.TaskService,
	executionService *service.ExecutionService,
) (*mcp.CallToolResult, LoadDataOutput, error) {
	if input.User == "" {
		input.User = "root"
	}
	if input.Port == 0 {
		input.Port = 26257
	}
	if input.InsertType == "" {
		input.InsertType = "insert"
	} else {
		validTypes := map[string]bool{
			"insert":     true,
			"prepare":    true,
			"prepareiot": true,
		}
		if !validTypes[input.InsertType] {
			return &mcp.CallToolResult{IsError: true}, LoadDataOutput{}, fmt.Errorf("invalid insert_type: %s, must be one of: insert, prepare, prepareiot", input.InsertType)
		}
	}
	if input.Partition == nil {
		partition := false
		input.Partition = &partition
	}

	taskID, err := taskService.CreateTask(ctx, "load", input)
	if err != nil {
		return &mcp.CallToolResult{IsError: true}, LoadDataOutput{}, err
	}

	serviceInput := service.LoadDataInput{
		File:        input.File,
		User:        input.User,
		Password:    input.Password,
		Host:        input.Host,
		Port:        input.Port,
		InsertType:  input.InsertType,
		DBName:      input.DBName,
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
	req *mcp.CallToolRequest,
	input GenerateQueriesInput,
	taskService *service.TaskService,
	executionService *service.ExecutionService,
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

	// 验证时间间隔是否足够大（根据查询类型）
	// 解析时间戳
	startTime, err := time.Parse(time.RFC3339, input.TimestampStart)
	if err != nil {
		return &mcp.CallToolResult{IsError: true}, GenerateQueriesOutput{}, fmt.Errorf("invalid timestamp-start format: %v", err)
	}
	endTime, err := time.Parse(time.RFC3339, input.TimestampEnd)
	if err != nil {
		return &mcp.CallToolResult{IsError: true}, GenerateQueriesOutput{}, fmt.Errorf("invalid timestamp-end format: %v", err)
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
		return &mcp.CallToolResult{IsError: true}, GenerateQueriesOutput{},
			fmt.Errorf("time interval too small: got %v, need at least %v for query type %s. Please increase the time range between timestamp-start and timestamp-end",
				timeInterval, minInterval, input.QueryType)
	}

	taskID, err := taskService.CreateTask(ctx, "generate_queries", input)
	if err != nil {
		return &mcp.CallToolResult{IsError: true}, GenerateQueriesOutput{}, err
	}

	serviceInput := service.GenerateQueriesInput{
		UseCase:        input.UseCase,
		Seed:           input.Seed,
		Scale:          input.Scale,
		QueryType:      input.QueryType,
		Format:         input.Format,
		Queries:        input.Queries,
		DBName:         input.DBName,
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
	req *mcp.CallToolRequest,
	input RunQueriesInput,
	taskService *service.TaskService,
	executionService *service.ExecutionService,
) (*mcp.CallToolResult, RunQueriesOutput, error) {
	// 参数验证和默认值（参考 scripts/tsbs_kwdb.sh）
	if input.User == "" {
		input.User = "root" // 脚本中固定使用 root
	}
	if input.Port == 0 {
		input.Port = 26257 // KWDB 默认端口
	}
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
		return &mcp.CallToolResult{IsError: true}, RunQueriesOutput{}, err
	}

	// 转换为 service 包的类型
	serviceInput := service.RunQueriesInput{
		File:             input.File,
		User:             input.User,
		Password:         input.Password,
		Host:             input.Host,
		Port:             input.Port,
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
	req *mcp.CallToolRequest,
	input StatusInput,
	statusService *service.StatusService,
) (*mcp.CallToolResult, StatusOutput, error) {
	status, err := statusService.GetTaskStatus(ctx, input.TestTaskID)
	if err != nil {
		return &mcp.CallToolResult{IsError: true}, StatusOutput{}, err
	}

	// 转换为 mcp_tools 的类型
	output := StatusOutput{
		Status:     status.Status,
		Progress:   status.Progress,
		Message:    status.Message,
		OutputFile: status.OutputFile,
		Error:      status.Error,
		Result:     status.Result,
		Metrics:    status.Metrics,
	}

	return nil, output, nil
}

func handleGetLoadStatus(
	ctx context.Context,
	req *mcp.CallToolRequest,
	input StatusInput,
	statusService *service.StatusService,
) (*mcp.CallToolResult, StatusOutput, error) {
	status, err := statusService.GetTaskStatus(ctx, input.TestTaskID)
	if err != nil {
		return &mcp.CallToolResult{IsError: true}, StatusOutput{}, err
	}

	// 转换为 mcp_tools 的类型
	output := StatusOutput{
		Status:     status.Status,
		Progress:   status.Progress,
		Message:    status.Message,
		OutputFile: status.OutputFile,
		Error:      status.Error,
		Result:     status.Result,
		Metrics:    status.Metrics,
	}

	return nil, output, nil
}

func handleGetGenerateQueriesStatus(
	ctx context.Context,
	req *mcp.CallToolRequest,
	input StatusInput,
	statusService *service.StatusService,
) (*mcp.CallToolResult, StatusOutput, error) {
	status, err := statusService.GetTaskStatus(ctx, input.TestTaskID)
	if err != nil {
		return &mcp.CallToolResult{IsError: true}, StatusOutput{}, err
	}

	// 转换为 mcp_tools 的类型
	output := StatusOutput{
		Status:     status.Status,
		Progress:   status.Progress,
		Message:    status.Message,
		OutputFile: status.OutputFile,
		Error:      status.Error,
		Result:     status.Result,
		Metrics:    status.Metrics,
	}

	return nil, output, nil
}

func handleGetRunQueriesStatus(
	ctx context.Context,
	req *mcp.CallToolRequest,
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
		return &mcp.CallToolResult{IsError: true}, StatusOutput{}, err
	}

	// 转换为 mcp_tools 的类型
	output := StatusOutput{
		Status:     serviceStatus.Status,
		Progress:   serviceStatus.Progress,
		Message:    serviceStatus.Message,
		OutputFile: serviceStatus.OutputFile,
		Error:      serviceStatus.Error,
		Result:     serviceStatus.Result,
		Metrics:    serviceStatus.Metrics,
	}

	return nil, output, nil
}
