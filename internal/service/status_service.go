package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/timescale/tsbs/internal/db"
)

type StatusService struct {
	db *db.Connection
}

func NewStatusService(conn *db.Connection) *StatusService {
	return &StatusService{db: conn}
}

func (s *StatusService) GetTaskStatus(ctx context.Context, taskID string) (StatusOutput, error) {
	query := `
		SELECT status, progress, error_message, result, output_file
		FROM tsbs_test_tasks
		WHERE task_id = $1
	`

	var status string
	var progress int
	var errorMsg sql.NullString
	var result sql.NullString
	var outputFile sql.NullString

	err := s.db.DB.QueryRowContext(ctx, query, taskID).Scan(&status, &progress, &errorMsg, &result, &outputFile)
	if err == sql.ErrNoRows {
		return StatusOutput{}, fmt.Errorf("task not found: %s", taskID)
	}
	if err != nil {
		return StatusOutput{}, fmt.Errorf("failed to get task: %w", err)
	}

	output := StatusOutput{
		Status:   status,
		Progress: progress,
	}

	if outputFile.Valid {
		output.OutputFile = outputFile.String
	}

	if errorMsg.Valid {
		output.Error = errorMsg.String
	}

	if result.Valid && len(result.String) > 0 {
		output.Result = json.RawMessage(result.String)
	}

	return output, nil
}

func (s *StatusService) GetSubtaskStatus(ctx context.Context, taskID, subtaskID string) (StatusOutput, error) {
	query := `
		SELECT status, progress, result
		FROM tsbs_test_subtasks
		WHERE task_id = $1 AND subtask_id = $2
	`

	var status string
	var progress int
	var result sql.NullString

	err := s.db.DB.QueryRowContext(ctx, query, taskID, subtaskID).Scan(&status, &progress, &result)
	if err == sql.ErrNoRows {
		return StatusOutput{}, fmt.Errorf("subtask not found: %s", subtaskID)
	}
	if err != nil {
		return StatusOutput{}, fmt.Errorf("failed to get subtask: %w", err)
	}

	output := StatusOutput{
		Status:   status,
		Progress: progress,
	}

	if result.Valid && len(result.String) > 0 {
		output.Result = json.RawMessage(result.String)
	}

	return output, nil
}
