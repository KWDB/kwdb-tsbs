package config

import (
	"fmt"
	"os"
	"strconv"

	"gopkg.in/yaml.v2"
)

type Config struct {
	Server   ServerConfig   `yaml:"server"`
	Database DatabaseConfig `yaml:"database"`
	TSBS     TSBSConfig     `yaml:"tsbs"`
}

type ServerConfig struct {
	Port int    `yaml:"port"`
	Host string `yaml:"host"`
}

type DatabaseConfig struct {
	Host        string `yaml:"host"`        // 数据库主机地址
	Port        int    `yaml:"port"`        // 数据库端口
	User        string `yaml:"user"`        // 数据库用户名
	Password    string `yaml:"password"`    // 数据库密码（建议使用环境变量或加密存储）
	DBName      string `yaml:"dbname"`      // 数据库名称
	SSLMode     string `yaml:"sslmode"`     // SSL 模式
	SSLCert     string `yaml:"sslcert"`     // SSL 客户端证书文件路径（可选）
	SSLKey      string `yaml:"sslkey"`      // SSL 客户端密钥文件路径（可选）
	SSLRootCert string `yaml:"sslrootcert"` // SSL 根证书文件路径（可选，用于 verify-ca 和 verify-full）
}

type TSBSConfig struct {
	BinPath    string `yaml:"bin_path"`    // TSBS 二进制文件路径
	WorkDir    string `yaml:"work_dir"`    // 工作目录
	DataDir    string `yaml:"data_dir"`    // 数据目录
	QueryDir   string `yaml:"query_dir"`   // 查询目录
	ReportsDir string `yaml:"reports_dir"` // 报告目录
	// 可选：TSBS 配置文件路径，用于读取测试目标数据库的默认配置
	// 如果指定，可以从 TSBS 配置文件中读取 loader.db-specific 部分的配置作为默认值
	TSBSConfigPath string `yaml:"tsbs_config_path,omitempty"` // TSBS 配置文件路径（可选）
}

func Load() (*Config, error) {
	cfg := &Config{}

	// 默认值
	cfg.Server.Port = 8080
	cfg.Server.Host = "0.0.0.0"
	cfg.Database.Host = "localhost"
	cfg.Database.Port = 5432
	cfg.Database.User = "postgres"
	cfg.Database.Password = ""
	cfg.Database.DBName = "tsbs"
	cfg.Database.SSLMode = "disable"
	cfg.TSBS.BinPath = "./bin"
	cfg.TSBS.WorkDir = "./tsbs_work"
	cfg.TSBS.DataDir = "./tsbs_work/load_data"
	cfg.TSBS.QueryDir = "./tsbs_work/query_data"
	cfg.TSBS.ReportsDir = "./tsbs_work/reports"

	// 从环境变量读取
	if port := os.Getenv("TSBS_SERVER_PORT"); port != "" {
		if p, err := strconv.Atoi(port); err == nil {
			cfg.Server.Port = p
		}
	}

	if host := os.Getenv("TSBS_SERVER_HOST"); host != "" {
		cfg.Server.Host = host
	}

	if dbHost := os.Getenv("TSBS_DB_HOST"); dbHost != "" {
		cfg.Database.Host = dbHost
	}

	if dbPort := os.Getenv("TSBS_DB_PORT"); dbPort != "" {
		if p, err := strconv.Atoi(dbPort); err == nil {
			cfg.Database.Port = p
		}
	}

	if dbUser := os.Getenv("TSBS_DB_USER"); dbUser != "" {
		cfg.Database.User = dbUser
	}

	if dbPass := os.Getenv("TSBS_DB_PASSWORD"); dbPass != "" {
		cfg.Database.Password = dbPass
	}

	if dbName := os.Getenv("TSBS_DB_NAME"); dbName != "" {
		cfg.Database.DBName = dbName
	}

	if binPath := os.Getenv("TSBS_BIN_PATH"); binPath != "" {
		cfg.TSBS.BinPath = binPath
	}

	if workDir := os.Getenv("TSBS_WORK_DIR"); workDir != "" {
		cfg.TSBS.WorkDir = workDir
		cfg.TSBS.DataDir = workDir + "/load_data"
		cfg.TSBS.QueryDir = workDir + "/query_data"
		cfg.TSBS.ReportsDir = workDir + "/reports"
	}

	// 从配置文件读取（如果存在）
	configPath := os.Getenv("TSBS_CONFIG_PATH")
	if configPath == "" {
		configPath = "./configs/config.yaml"
	}

	if _, err := os.Stat(configPath); err == nil {
		data, err := os.ReadFile(configPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read config file: %w", err)
		}

		if err := yaml.Unmarshal(data, cfg); err != nil {
			return nil, fmt.Errorf("failed to parse config file: %w", err)
		}
	}

	// 验证配置
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("config validation failed: %w", err)
	}

	// 创建必要的目录
	if err := os.MkdirAll(cfg.TSBS.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	if err := os.MkdirAll(cfg.TSBS.QueryDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create query directory: %w", err)
	}

	if err := os.MkdirAll(cfg.TSBS.ReportsDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create reports directory: %w", err)
	}

	return cfg, nil
}

func (c *Config) Validate() error {
	if c.Server.Port <= 0 || c.Server.Port > 65535 {
		return fmt.Errorf("invalid server port: %d", c.Server.Port)
	}

	if c.Database.Host == "" {
		return fmt.Errorf("database host is required")
	}

	if c.Database.Port <= 0 || c.Database.Port > 65535 {
		return fmt.Errorf("invalid database port: %d", c.Database.Port)
	}

	if c.Database.User == "" {
		return fmt.Errorf("database user is required")
	}

	if c.Database.DBName == "" {
		return fmt.Errorf("database name is required")
	}

	if c.TSBS.BinPath == "" {
		return fmt.Errorf("TSBS bin path is required")
	}

	return nil
}

func (c *Config) GetDSN() string {
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		c.Database.Host,
		c.Database.Port,
		c.Database.User,
		c.Database.Password,
		c.Database.DBName,
		c.Database.SSLMode,
	)
}
