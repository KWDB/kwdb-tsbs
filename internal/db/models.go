package db

import (
	"database/sql"
	"encoding/json"
	"time"
)

// Task 测试任务
type Task struct {
	ID          int64           `db:"id"`
	TaskID      string          `db:"task_id"`
	TaskType    string          `db:"task_type"`
	Status      string          `db:"status"`
	Progress    int             `db:"progress"`
	CreatedAt   time.Time       `db:"created_at"`
	UpdatedAt   time.Time       `db:"updated_at"`
	StartedAt   sql.NullTime    `db:"started_at"`
	CompletedAt sql.NullTime    `db:"completed_at"`
	ErrorMessage sql.NullString `db:"error_message"`
	Config      json.RawMessage `db:"config"`
	Result      json.RawMessage `db:"result"`
	OutputFile  sql.NullString  `db:"output_file"`
}

// Subtask 子任务
type Subtask struct {
	ID         int64           `db:"id"`
	TaskID     string          `db:"task_id"`
	SubtaskID  string          `db:"subtask_id"`
	SubtaskType sql.NullString `db:"subtask_type"`
	Status     string          `db:"status"`
	Progress   int             `db:"progress"`
	CreatedAt  time.Time       `db:"created_at"`
	UpdatedAt  time.Time       `db:"updated_at"`
	Result     json.RawMessage `db:"result"`
}

// Result 测试结果
type Result struct {
	ID         int64           `db:"id"`
	TaskID     string          `db:"task_id"`
	ResultType string          `db:"result_type"`
	Metrics    json.RawMessage `db:"metrics"`
	CreatedAt  time.Time       `db:"created_at"`
}


