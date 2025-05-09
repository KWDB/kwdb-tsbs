package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"runtime/pprof"

	kwdb "github.com/timescale/tsbs/pkg/targets/kwdb"
	"github.com/timescale/tsbs/pkg/targets/kwdb/commonpool"

	"github.com/blagojts/viper"
	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/internal/utils"
	"github.com/timescale/tsbs/load"
	"github.com/timescale/tsbs/pkg/data/source"
)

func initProgramOptions() (*kwdb.LoadingOptions, load.BenchmarkRunner, *load.BenchmarkRunnerConfig) {
	target := kwdb.NewTarget()
	loaderConf := load.BenchmarkRunnerConfig{}
	loaderConf.AddToFlagSet(pflag.CommandLine)
	target.TargetSpecificFlags("", pflag.CommandLine)
	pflag.Parse()
	err := utils.SetupConfigFile()

	if err != nil {
		panic(fmt.Errorf("fatal error config file: %s", err))
	}

	if err := viper.Unmarshal(&loaderConf); err != nil {
		panic(fmt.Errorf("unable to decode config: %s", err))
	}
	opts := kwdb.LoadingOptions{}
	viper.SetTypeByDefaultValue(true)
	opts.User = viper.GetString("user")
	opts.Pass = viper.GetString("pass")
	opts.Host = viper.GetString("host")
	opts.Port = viper.GetInt("port")
	opts.DBName = viper.GetString("db-name")
	opts.Type = viper.GetString("insert-type")
	opts.Case = viper.GetString("case")
	opts.Workers = viper.GetInt("workers")
	opts.DoCreate = viper.GetBool("do-create-db")
	opts.Preparesize = viper.GetInt("preparesize")
	opts.CertDir = viper.GetString("certdir")
	opts.Partition = viper.GetBool("partition")
	loaderConf.HashWorkers = true
	loaderConf.NoFlowControl = true
	loaderConf.ChannelCapacity = 50
	loaderConf.DBName = opts.DBName
	loader := load.GetBenchmarkRunner(loaderConf)
	return &opts, loader, &loaderConf
}
func main() {
	f, err := os.Create("./cpu.prof")
	if err != nil {
		log.Fatal("could not create CPU profile: ", err)
	}
	if err := pprof.StartCPUProfile(f); err != nil {
		log.Fatal("could not start CPU profile: ", err)
	}
	defer pprof.StopCPUProfile()
	opts, loader, loaderConf := initProgramOptions()
	benchmark, err := kwdb.NewBenchmark(loaderConf.DBName, opts, &source.DataSourceConfig{
		Type: source.FileDataSourceType,
		File: &source.FileDataSourceConfig{Location: loaderConf.FileName},
	})
	if err != nil {
		panic(err)
	}
	loader.RunBenchmark(benchmark)

	_db, err := commonpool.GetConnection(opts.User, opts.Pass, opts.Host, opts.CertDir, opts.Port)

	if err != nil {
		panic(err)
	}
	ctx := context.Background()
	_, err = _db.Connection.Exec(ctx, "ALTER TABLE benchmark.cpu INJECT STATISTICS '[{\"columns\": [\"k_timestamp\"],\"created_at\": \"2024-09-06\",\"row_count\": 10000000,\"distinct_count\": 10000000,\"null_count\": 0}]';")
	if err != nil {
		//	panic(err)
	}
}
