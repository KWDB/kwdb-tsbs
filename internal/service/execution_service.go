package service

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/timescale/tsbs/internal/config"
	"github.com/timescale/tsbs/internal/db"
	"github.com/timescale/tsbs/internal/executor"
)

type ExecutionService struct {
	db     *db.Connection
	config *config.Config
	exec   *executor.Executor
}

func NewExecutionService(conn *db.Connection, cfg *config.Config) *ExecutionService {
	return &ExecutionService{
		db:     conn,
		config: cfg,
		exec:   executor.NewExecutor(cfg.TSBS.BinPath),
	}
}

func (s *ExecutionService) ExecuteGenerateData(ctx context.Context, taskID string, input GenerateDataInput) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("PANIC in ExecuteGenerateData for task %s: %v\n", taskID, r)
		}
	}()

	cmdCtx, cmdCancel := context.WithCancel(context.Background())
	defer cmdCancel()
	bgCtx := context.Background()
	taskService := NewTaskService(s.db)

	format := input.Format
	if format == "" {
		format = "kwdb"
	}

	outputFile := input.OutputFile
	if outputFile == nil || *outputFile == "" {
		filename := fmt.Sprintf("%s_%s_scale_%d_%dorder.dat",
			input.UseCase, format, input.Scale, getOrderQuantity(input.OrderQuantity))
		fullPath := filepath.Join(s.config.TSBS.DataDir, filename)
		if outputFile == nil {
			outputFile = &fullPath
		} else {
			*outputFile = fullPath
		}
		input.OutputFile = outputFile
	}

	args := []string{
		"--format=" + format,
		"--use-case=" + input.UseCase,
		fmt.Sprintf("--seed=%d", input.Seed),
		fmt.Sprintf("--scale=%d", input.Scale),
		"--log-interval=" + input.LogInterval,
		"--timestamp-start=" + input.TimestampStart,
		"--timestamp-end=" + input.TimestampEnd,
	}

	if input.OrderQuantity != nil {
		args = append(args, fmt.Sprintf("--orderquantity=%d", *input.OrderQuantity))
	}

	if input.UseCase == "cpu-only" {
		if input.OutOfOrder != nil {
			args = append(args, fmt.Sprintf("--outoforder=%f", *input.OutOfOrder))
		}
		if input.OutOfOrderWindow != nil {
			args = append(args, "--outoforderwindow="+*input.OutOfOrderWindow)
		}
	}

	// 执行命令
	binPath := filepath.Join(s.config.TSBS.BinPath, "tsbs_generate_data")
	// 确保使用绝对路径
	if !filepath.IsAbs(binPath) {
		absBinPath, err := filepath.Abs(binPath)
		if err == nil {
			binPath = absBinPath
		}
	}
	if _, err := os.Stat(binPath); os.IsNotExist(err) {
		taskService.FailTask(bgCtx, taskID, fmt.Sprintf("Binary file not found: %s", binPath))
		return
	}

	cmd := exec.CommandContext(cmdCtx, binPath, args...)

	outFile, err := os.Create(*outputFile)
	if err != nil {
		taskService.FailTask(bgCtx, taskID, fmt.Sprintf("Failed to create output file: %v", err))
		return
	}
	defer outFile.Close()

	cmd.Stdout = outFile
	cmd.Stderr = outFile

	if err := cmd.Start(); err != nil {
		taskService.FailTask(bgCtx, taskID, fmt.Sprintf("Failed to start command: %v", err))
		return
	}

	taskService.UpdateTaskProgress(bgCtx, taskID, 5)

	// 等待完成
	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
	}()

	// 定期更新进度
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	// 记录上次文件大小，用于判断文件是否在增长
	var lastFileSize int64 = -1 // 初始化为-1，以便第一次检查时能正确判断
	fileGrowing := false
	firstCheck := true

	for {
		select {
		case <-cmdCtx.Done():
			cmd.Process.Kill()
			taskService.FailTask(bgCtx, taskID, "Task cancelled")
			return
		case err := <-done:
			if err != nil {
				taskService.FailTask(bgCtx, taskID, fmt.Sprintf("Command failed: %v", err))
				return
			}
			result := map[string]interface{}{
				"output_file":  *outputFile,
				"completed_at": time.Now().Format(time.RFC3339),
			}
			taskService.CompleteTask(bgCtx, taskID, result, *outputFile)
			return
		case <-ticker.C:
			if stat, err := os.Stat(*outputFile); err == nil {
				currentSize := stat.Size()
				if firstCheck {
					firstCheck = false
					lastFileSize = currentSize
					if currentSize > 0 {
						taskService.UpdateTaskProgress(bgCtx, taskID, 30)
					} else {
						taskService.UpdateTaskProgress(bgCtx, taskID, 10)
					}
				} else if currentSize > lastFileSize {
					fileGrowing = true
					lastFileSize = currentSize
					taskService.UpdateTaskProgress(bgCtx, taskID, 50)
				} else if fileGrowing && currentSize == lastFileSize && currentSize > 0 {
					taskService.UpdateTaskProgress(bgCtx, taskID, 90)
				}
			} else if firstCheck {
				firstCheck = false
				taskService.UpdateTaskProgress(bgCtx, taskID, 5)
			}
		}
	}
}

func (s *ExecutionService) ExecuteLoadData(ctx context.Context, taskID string, input LoadDataInput) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("PANIC in ExecuteLoadData for task %s: %v\n", taskID, r)
		}
	}()

	cmdCtx, cmdCancel := context.WithCancel(context.Background())
	defer cmdCancel()
	bgCtx := context.Background()
	taskService := NewTaskService(s.db)

	args := []string{
		"--file=" + input.File,
		"--user=" + input.User,
		"--pass=" + input.Password,
		"--host=" + input.Host,
		fmt.Sprintf("--port=%d", input.Port),
		"--insert-type=" + input.InsertType,
		"--db-name=" + input.DBName,
		"--case=" + input.Case,
	}

	if input.InsertType == "prepare" || input.InsertType == "prepareiot" {
		if input.Preparesize != nil {
			args = append(args, fmt.Sprintf("--preparesize=%d", *input.Preparesize))
		} else if input.BatchSize != nil {
			args = append(args, fmt.Sprintf("--preparesize=%d", *input.BatchSize))
		}
	} else {
		if input.BatchSize != nil {
			args = append(args, fmt.Sprintf("--batch-size=%d", *input.BatchSize))
		}
	}

	if input.Workers != nil {
		args = append(args, fmt.Sprintf("--workers=%d", *input.Workers))
	}

	if input.Partition != nil {
		args = append(args, fmt.Sprintf("--partition=%v", *input.Partition))
	}

	binPath := filepath.Join(s.config.TSBS.BinPath, "tsbs_load_kwdb")
	if !filepath.IsAbs(binPath) {
		if absBinPath, err := filepath.Abs(binPath); err == nil {
			binPath = absBinPath
		}
	}

	if _, err := os.Stat(binPath); os.IsNotExist(err) {
		taskService.FailTask(bgCtx, taskID, fmt.Sprintf("Binary file not found: %s", binPath))
		return
	}

	cmd := exec.CommandContext(cmdCtx, binPath, args...)
	result, err := s.exec.ExecuteWithOutput(cmdCtx, cmd)
	if err != nil {
		errorMsg := fmt.Sprintf("Command failed: %v", err)
		if result != nil && len(result.Output) > 0 {
			errorOutput := result.Output
			if len(errorOutput) > 1000 {
				errorOutput = errorOutput[:1000] + "... (truncated)"
			}
			errorMsg = fmt.Sprintf("Command failed: %v\nOutput: %s", err, errorOutput)
		}
		taskService.FailTask(bgCtx, taskID, errorMsg)
		return
	}

	metrics := parseLoadMetrics(result.Output)
	taskResult := map[string]interface{}{
		"metrics":      metrics,
		"output":       result.Output,
		"completed_at": time.Now().Format(time.RFC3339),
	}
	taskService.CompleteTask(bgCtx, taskID, taskResult, "")
}

func (s *ExecutionService) ExecuteGenerateQueries(ctx context.Context, taskID string, input GenerateQueriesInput) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("PANIC in ExecuteGenerateQueries for task %s: %v\n", taskID, r)
		}
	}()

	cmdCtx, cmdCancel := context.WithCancel(context.Background())
	defer cmdCancel()
	bgCtx := context.Background()
	taskService := NewTaskService(s.db)

	format := input.Format
	if format == "" {
		format = "kwdb"
	}

	outputFile := input.OutputFile
	if outputFile == nil || *outputFile == "" {
		filename := fmt.Sprintf("%s_scale%d_%s_%s_query_times%d.dat",
			format, input.Scale, input.UseCase, input.QueryType, input.Queries)
		// 脚本中查询文件存储在 query_data/scale${scale}/ 目录下
		queryDir := filepath.Join(s.config.TSBS.QueryDir, fmt.Sprintf("scale%d", input.Scale))
		os.MkdirAll(queryDir, 0755)
		fullPath := filepath.Join(queryDir, filename)
		if outputFile == nil {
			outputFile = &fullPath
		} else {
			*outputFile = fullPath
		}
		input.OutputFile = outputFile
	}

	args := []string{
		"--format=" + format,
		"--use-case=" + input.UseCase,
		fmt.Sprintf("--seed=%d", input.Seed),
		fmt.Sprintf("--scale=%d", input.Scale),
		"--query-type=" + input.QueryType,
		fmt.Sprintf("--queries=%d", input.Queries),
		"--db-name=" + input.DBName,
		"--timestamp-start=" + input.TimestampStart,
		"--timestamp-end=" + input.TimestampEnd,
	}

	if input.Prepare != nil {
		args = append(args, fmt.Sprintf("--prepare=%v", *input.Prepare))
	} else {
		args = append(args, "--prepare=false")
	}

	binPath := filepath.Join(s.config.TSBS.BinPath, "tsbs_generate_queries")
	if !filepath.IsAbs(binPath) {
		if absBinPath, err := filepath.Abs(binPath); err == nil {
			binPath = absBinPath
		}
	}

	if _, err := os.Stat(binPath); os.IsNotExist(err) {
		taskService.FailTask(bgCtx, taskID, fmt.Sprintf("Binary file not found: %s", binPath))
		return
	}

	cmd := exec.CommandContext(cmdCtx, binPath, args...)
	outFile, err := os.Create(*outputFile)
	if err != nil {
		taskService.FailTask(bgCtx, taskID, fmt.Sprintf("Failed to create output file: %v", err))
		return
	}
	defer outFile.Close()

	cmd.Stdout = outFile

	if err := cmd.Start(); err != nil {
		taskService.FailTask(bgCtx, taskID, fmt.Sprintf("Failed to start command: %v", err))
		return
	}

	taskService.UpdateTaskProgress(bgCtx, taskID, 5)

	// 等待完成
	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
	}()

	// 定期更新进度
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	var lastFileSize int64 = -1 // 初始化为-1，以便第一次检查时能正确判断
	fileGrowing := false
	firstCheck := true

	for {
		select {
		case <-cmdCtx.Done():
			if cmd.Process != nil {
				cmd.Process.Kill()
			}
			taskService.FailTask(bgCtx, taskID, "Task cancelled")
			return
		case err := <-done:
			if err != nil {
				// 命令失败，尝试读取输出文件获取错误信息
				errorMsg := fmt.Sprintf("Command failed: %v", err)
				// 读取输出文件内容（包含 stderr）
				if content, readErr := os.ReadFile(*outputFile); readErr == nil && len(content) > 0 {
					// 限制错误信息长度，避免过长
					errorOutput := string(content)
					if len(errorOutput) > 1000 {
						errorOutput = errorOutput[:1000] + "... (truncated)"
					}
					errorMsg = fmt.Sprintf("Command failed: %v\nOutput: %s", err, errorOutput)
				} else if stat, statErr := os.Stat(*outputFile); statErr == nil && stat.Size() == 0 {
					errorMsg = fmt.Sprintf("Command failed: %v (output file is empty)", err)
				}
				fmt.Printf("[ExecuteGenerateQueries] ERROR: %s\n", errorMsg)
				if failErr := taskService.FailTask(bgCtx, taskID, errorMsg); failErr != nil {
					fmt.Printf("[ExecuteGenerateQueries] ERROR: Failed to update task status: %v\n", failErr)
				} else {
					fmt.Printf("[ExecuteGenerateQueries] Task %s marked as failed\n", taskID)
				}
				return
			}
			// 成功完成
			fmt.Printf("[ExecuteGenerateQueries] Command completed successfully for task %s\n", taskID)
			// 检查输出文件是否存在且有内容
			var stat os.FileInfo
			var statErr error
			if stat, statErr = os.Stat(*outputFile); statErr != nil {
				errorMsg := fmt.Sprintf("Output file not found after command completion: %v", statErr)
				fmt.Printf("[ExecuteGenerateQueries] ERROR: %s\n", errorMsg)
				if failErr := taskService.FailTask(bgCtx, taskID, errorMsg); failErr != nil {
					fmt.Printf("[ExecuteGenerateQueries] ERROR: Failed to update task status: %v\n", failErr)
				}
				return
			} else if stat.Size() == 0 {
				errorMsg := "Output file is empty after command completion"
				fmt.Printf("[ExecuteGenerateQueries] ERROR: %s\n", errorMsg)
				if failErr := taskService.FailTask(bgCtx, taskID, errorMsg); failErr != nil {
					fmt.Printf("[ExecuteGenerateQueries] ERROR: Failed to update task status: %v\n", failErr)
				}
				return
			}
			result := map[string]interface{}{
				"output_file":  *outputFile,
				"file_size":    stat.Size(),
				"completed_at": time.Now().Format(time.RFC3339),
			}
			if compErr := taskService.CompleteTask(bgCtx, taskID, result, *outputFile); compErr != nil {
				fmt.Printf("[ExecuteGenerateQueries] ERROR: Failed to complete task %s: %v\n", taskID, compErr)
			} else {
				fmt.Printf("[ExecuteGenerateQueries] Successfully completed task %s\n", taskID)
			}
			return
		case <-ticker.C:
			if stat, err := os.Stat(*outputFile); err == nil {
				currentSize := stat.Size()
				if firstCheck {
					firstCheck = false
					lastFileSize = currentSize
					if currentSize > 0 {
						taskService.UpdateTaskProgress(bgCtx, taskID, 30)
					} else {
						taskService.UpdateTaskProgress(bgCtx, taskID, 10)
					}
				} else if currentSize > lastFileSize {
					fileGrowing = true
					lastFileSize = currentSize
					taskService.UpdateTaskProgress(bgCtx, taskID, 50)
				} else if fileGrowing && currentSize == lastFileSize && currentSize > 0 {
					taskService.UpdateTaskProgress(bgCtx, taskID, 90)
				}
			} else if firstCheck {
				firstCheck = false
				taskService.UpdateTaskProgress(bgCtx, taskID, 5)
			}
		}
	}
}

func (s *ExecutionService) ExecuteRunQueries(ctx context.Context, taskID string, input RunQueriesInput) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("PANIC in ExecuteRunQueries for task %s: %v\n", taskID, r)
		}
	}()

	cmdCtx, cmdCancel := context.WithCancel(context.Background())
	defer cmdCancel()
	bgCtx := context.Background()
	taskService := NewTaskService(s.db)

	args := []string{
		"--file=" + input.File,
		"--user=" + input.User,
		"--pass=" + input.Password,
		"--host=" + input.Host,
		fmt.Sprintf("--port=%d", input.Port),
		"--query-type=" + input.QueryType,
	}

	if input.Workers != nil {
		args = append(args, fmt.Sprintf("--workers=%d", *input.Workers))
	} else {
		args = append(args, "--workers=1")
	}

	if input.Prepare != nil {
		args = append(args, fmt.Sprintf("--prepare=%v", *input.Prepare))
	} else {
		args = append(args, "--prepare=false")
	}

	binPath := filepath.Join(s.config.TSBS.BinPath, "tsbs_run_queries_kwdb")
	if !filepath.IsAbs(binPath) {
		if absBinPath, err := filepath.Abs(binPath); err == nil {
			binPath = absBinPath
		}
	}

	if _, err := os.Stat(binPath); os.IsNotExist(err) {
		taskService.FailTask(bgCtx, taskID, fmt.Sprintf("Binary file not found: %s", binPath))
		return
	}

	if _, err := os.Stat(input.File); os.IsNotExist(err) {
		taskService.FailTask(bgCtx, taskID, fmt.Sprintf("Query file not found: %s", input.File))
		return
	}

	cmd := exec.CommandContext(cmdCtx, binPath, args...)
	result, err := s.exec.ExecuteWithOutput(cmdCtx, cmd)
	if err != nil {
		errorMsg := fmt.Sprintf("Command failed: %v", err)
		if result != nil && len(result.Output) > 0 {
			errorOutput := result.Output
			if len(errorOutput) > 1000 {
				errorOutput = errorOutput[:1000] + "... (truncated)"
			}
			errorMsg = fmt.Sprintf("Command failed: %v\nOutput: %s", err, errorOutput)
		}
		taskService.FailTask(bgCtx, taskID, errorMsg)
		return
	}

	metrics := parseQueryMetrics(result.Output)
	taskResult := map[string]interface{}{
		"metrics":      metrics,
		"output":       result.Output,
		"completed_at": time.Now().Format(time.RFC3339),
	}
	taskService.CompleteTask(bgCtx, taskID, taskResult, "")
}

// 辅助函数
func getOrderQuantity(q *int) int {
	if q == nil {
		return 12
	}
	return *q
}

func parseLoadMetrics(output string) map[string]interface{} {
	// 从输出中解析性能指标
	// 这里简化处理，实际需要解析 tsbs_load_kwdb 的输出格式
	return map[string]interface{}{
		"output": output,
	}
}

func parseQueryMetrics(output string) map[string]interface{} {
	// 从输出中解析查询性能指标
	// 这里简化处理，实际需要解析 tsbs_run_queries_kwdb 的输出格式
	return map[string]interface{}{
		"output": output,
	}
}
