package db

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
	"github.com/timescale/tsbs/internal/config"
)

type Connection struct {
	DB *sql.DB
}

func NewConnection(cfg config.DatabaseConfig) (*Connection, error) {
	dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		cfg.Host, cfg.Port, cfg.User, cfg.Password, cfg.DBName, cfg.SSLMode)

	// 添加 SSL 证书配置（如果提供）
	if cfg.SSLCert != "" {
		dsn += fmt.Sprintf(" sslcert=%s", cfg.SSLCert)
	}
	if cfg.SSLKey != "" {
		dsn += fmt.Sprintf(" sslkey=%s", cfg.SSLKey)
	}
	if cfg.SSLRootCert != "" {
		dsn += fmt.Sprintf(" sslrootcert=%s", cfg.SSLRootCert)
	}

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	// 设置连接池参数
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)

	return &Connection{DB: db}, nil
}

func (c *Connection) Close() error {
	return c.DB.Close()
}
