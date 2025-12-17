package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/timescale/tsbs/internal/db"
)

type TaskService struct {
	db *db.Connection
}

func NewTaskService(conn *db.Connection) *TaskService {
	return &TaskService{db: conn}
}

func (s *TaskService) CreateTask(ctx context.Context, taskType string, config interface{}) (string, error) {
	taskID := uuid.New().String()

	configJSON, err := json.Marshal(config)
	if err != nil {
		return "", fmt.Errorf("failed to marshal config: %w", err)
	}

	query := `
		INSERT INTO tsbs_test_tasks (task_id, task_type, status, progress, config, started_at)
		VALUES ($1, $2, $3, $4, $5, NOW())
		RETURNING task_id
	`

	var result string
	err = s.db.DB.QueryRowContext(ctx, query, taskID, taskType, "running", 0, configJSON).Scan(&result)
	if err != nil {
		return "", fmt.Errorf("failed to create task: %w", err)
	}

	return result, nil
}

func (s *TaskService) UpdateTaskStatus(ctx context.Context, taskID, status string) error {
	query := `
		UPDATE tsbs_test_tasks 
		SET status = $1, updated_at = NOW()
		WHERE task_id = $2
	`

	_, err := s.db.DB.ExecContext(ctx, query, status, taskID)
	return err
}

func (s *TaskService) UpdateTaskProgress(ctx context.Context, taskID string, progress int) error {
	query := `
		UPDATE tsbs_test_tasks 
		SET progress = $1, updated_at = NOW()
		WHERE task_id = $2
	`

	_, err := s.db.DB.ExecContext(ctx, query, progress, taskID)
	return err
}

func (s *TaskService) CompleteTask(ctx context.Context, taskID string, result interface{}, outputFile string) error {
	resultJSON, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("failed to marshal result: %w", err)
	}

	query := `
		UPDATE tsbs_test_tasks 
		SET status = 'completed', 
		    progress = 100,
		    completed_at = NOW(),
		    result = $1,
		    output_file = $2,
		    updated_at = NOW()
		WHERE task_id = $3
	`

	_, err = s.db.DB.ExecContext(ctx, query, resultJSON, outputFile, taskID)
	return err
}

func (s *TaskService) FailTask(ctx context.Context, taskID string, errorMsg string) error {
	query := `
		UPDATE tsbs_test_tasks 
		SET status = 'failed',
		    error_message = $1,
		    completed_at = NOW(),
		    updated_at = NOW()
		WHERE task_id = $2
	`

	_, err := s.db.DB.ExecContext(ctx, query, errorMsg, taskID)
	return err
}

func (s *TaskService) GetTask(ctx context.Context, taskID string) (*db.Task, error) {
	query := `
		SELECT id, task_id, task_type, status, progress, created_at, updated_at,
		       started_at, completed_at, error_message, config, result, output_file
		FROM tsbs_test_tasks
		WHERE task_id = $1
	`

	var task db.Task
	err := s.db.DB.QueryRowContext(ctx, query, taskID).Scan(
		&task.ID, &task.TaskID, &task.TaskType, &task.Status, &task.Progress,
		&task.CreatedAt, &task.UpdatedAt, &task.StartedAt, &task.CompletedAt,
		&task.ErrorMessage, &task.Config, &task.Result, &task.OutputFile,
	)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("task not found: %s", taskID)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get task: %w", err)
	}

	return &task, nil
}
