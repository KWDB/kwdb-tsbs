package migrate

import (
	"fmt"

	"github.com/timescale/tsbs/internal/db"
)

func Run(conn *db.Connection) error {
	// 创建表
	if err := createTables(conn); err != nil {
		return fmt.Errorf("failed to create tables: %w", err)
	}

	// 创建索引
	if err := createIndexes(conn); err != nil {
		return fmt.Errorf("failed to create indexes: %w", err)
	}

	return nil
}

func createTables(conn *db.Connection) error {
	// 创建主任务表
	createTasksTable := `
	CREATE TABLE IF NOT EXISTS tsbs_test_tasks (
		id SERIAL PRIMARY KEY,
		task_id VARCHAR(64) UNIQUE NOT NULL,
		task_type VARCHAR(32) NOT NULL,
		status VARCHAR(16) NOT NULL DEFAULT 'running',
		progress INT DEFAULT 0,
		created_at TIMESTAMP DEFAULT NOW(),
		updated_at TIMESTAMP DEFAULT NOW(),
		started_at TIMESTAMP,
		completed_at TIMESTAMP,
		error_message TEXT,
		config JSONB,
		result JSONB,
		output_file VARCHAR(512)
	);`

	if _, err := conn.DB.Exec(createTasksTable); err != nil {
		return fmt.Errorf("failed to create tsbs_test_tasks table: %w", err)
	}

	// 创建子任务表
	createSubtasksTable := `
	CREATE TABLE IF NOT EXISTS tsbs_test_subtasks (
		id SERIAL PRIMARY KEY,
		task_id VARCHAR(64) NOT NULL,
		subtask_id VARCHAR(64) UNIQUE NOT NULL,
		subtask_type VARCHAR(32),
		status VARCHAR(16) NOT NULL DEFAULT 'running',
		progress INT DEFAULT 0,
		created_at TIMESTAMP DEFAULT NOW(),
		updated_at TIMESTAMP DEFAULT NOW(),
		result JSONB,
		FOREIGN KEY (task_id) REFERENCES tsbs_test_tasks(task_id) ON DELETE CASCADE
	);`

	if _, err := conn.DB.Exec(createSubtasksTable); err != nil {
		return fmt.Errorf("failed to create tsbs_test_subtasks table: %w", err)
	}

	// 创建结果表
	createResultsTable := `
	CREATE TABLE IF NOT EXISTS tsbs_test_results (
		id SERIAL PRIMARY KEY,
		task_id VARCHAR(64) NOT NULL,
		result_type VARCHAR(32),
		metrics JSONB,
		created_at TIMESTAMP DEFAULT NOW(),
		FOREIGN KEY (task_id) REFERENCES tsbs_test_tasks(task_id) ON DELETE CASCADE
	);`

	if _, err := conn.DB.Exec(createResultsTable); err != nil {
		return fmt.Errorf("failed to create tsbs_test_results table: %w", err)
	}

	return nil
}

func createIndexes(conn *db.Connection) error {
	indexes := []string{
		"CREATE INDEX IF NOT EXISTS idx_tasks_task_id ON tsbs_test_tasks(task_id);",
		"CREATE INDEX IF NOT EXISTS idx_tasks_status ON tsbs_test_tasks(status);",
		"CREATE INDEX IF NOT EXISTS idx_tasks_task_type ON tsbs_test_tasks(task_type);",
		"CREATE INDEX IF NOT EXISTS idx_subtasks_task_id ON tsbs_test_subtasks(task_id);",
		"CREATE INDEX IF NOT EXISTS idx_subtasks_subtask_id ON tsbs_test_subtasks(subtask_id);",
		"CREATE INDEX IF NOT EXISTS idx_results_task_id ON tsbs_test_results(task_id);",
	}

	for _, idx := range indexes {
		if _, err := conn.DB.Exec(idx); err != nil {
			return fmt.Errorf("failed to create index: %w", err)
		}
	}

	return nil
}
