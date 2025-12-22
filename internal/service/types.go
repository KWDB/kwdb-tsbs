package service

// 执行服务的输入类型

type GenerateDataInput struct {
	UseCase          string
	Seed             int
	Scale            int
	LogInterval      string
	TimestampStart   string
	TimestampEnd     string
	Format           string
	OrderQuantity    *int
	OutOfOrder       *float64
	OutOfOrderWindow *string
	OutputFile       *string
}

type LoadDataInput struct {
	File        string
	User        string
	Password    string
	Host        string
	Port        int
	InsertType  string
	DBName      string
	Case        string
	BatchSize   *int
	Preparesize *int
	Workers     *int
	Partition   *bool
}

type GenerateQueriesInput struct {
	UseCase        string
	Seed           int
	Scale          int
	QueryType      string
	Format         string
	Queries        int
	DBName         string
	TimestampStart string
	TimestampEnd   string
	Prepare        *bool
	OutputFile     *string
}

type RunQueriesInput struct {
	File             string
	User             string
	Password         string
	Host             string
	Port             int
	Workers          *int
	Prepare          *bool
	QueryType        string
	ServerConfigName *string
}

// StatusOutput 用于状态查询的输出
type StatusOutput struct {
	Status     string
	Progress   int
	Message    string
	OutputFile string
	Error      string
	Result     []byte
	Metrics    []byte
}
