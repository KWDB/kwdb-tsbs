package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/timescale/tsbs/internal/config"
	"github.com/timescale/tsbs/internal/db"
	"github.com/timescale/tsbs/internal/mcp_tools"
	"github.com/timescale/tsbs/internal/migrate"
	"github.com/timescale/tsbs/internal/service"
)

func main() {
	// 1. 读取配置
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// 2. 创建数据库连接
	database, err := db.NewConnection(cfg.Database)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer database.Close()

	// 3. 执行数据库迁移
	if err := migrate.Run(database); err != nil {
		log.Fatalf("Failed to run migrations: %v", err)
	}

	// 4. 初始化服务
	taskService := service.NewTaskService(database)
	statusService := service.NewStatusService(database)
	executionService := service.NewExecutionService(database, cfg)

	// 5. 创建 MCP Server
	server := mcp.NewServer(
		&mcp.Implementation{
			Name:    "tsbs-mcp-server",
			Version: "1.0.0",
		},
		nil,
	)

	// 6. 注册所有工具
	mcp_tools.RegisterTools(server, taskService, statusService, executionService, cfg)

	// 7. 创建 Streamable HTTP Handler
	// 使用 Stateless 模式，不验证会话 ID，每次请求使用临时会话
	opts := &mcp.StreamableHTTPOptions{
		Stateless: true, // 无状态模式，避免会话管理问题
	}
	handler := mcp.NewStreamableHTTPHandler(
		func(r *http.Request) *mcp.Server {
			return server
		},
		opts,
	)

	// 8. 创建 HTTP 服务器
	mux := http.NewServeMux()
	mux.HandleFunc("/", handler.ServeHTTP)

	httpServer := &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.Server.Port),
		Handler: mux,
	}

	// 9. 启动服务器
	log.Printf("TSBS MCP Server starting on port %d", cfg.Server.Port)

	// 在 goroutine 中启动 HTTP 服务器
	go func() {
		log.Printf("HTTP server listening on :%d", cfg.Server.Port)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	// 等待中断信号
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down server...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := httpServer.Shutdown(ctx); err != nil {
		log.Fatalf("Server forced to shutdown: %v", err)
	}

	log.Println("Server exited")
}
