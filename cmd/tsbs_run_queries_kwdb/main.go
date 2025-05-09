package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/blagojts/viper"
	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/internal/utils"
	"github.com/timescale/tsbs/pkg/query"
	"github.com/timescale/tsbs/pkg/targets/kwdb/commonpool"
)

var (
	user    string
	pass    string
	host    string
	certdir string
	port    int
	runner  *query.BenchmarkRunner
)

func init() {
	var config query.BenchmarkRunnerConfig
	config.AddToFlagSet(pflag.CommandLine)

	pflag.String("user", "root", "User to connect to kwdb")
	pflag.String("pass", "", "Password for the user connecting to kwdb")
	pflag.String("host", "", "kwdb host")
	pflag.String("certdir", "", "dir of cert files")
	pflag.Int("port", 26257, "kwdb Port")
	pflag.Parse()
	err := utils.SetupConfigFile()

	if err != nil {
		panic(fmt.Errorf("fatal error config file: %s", err))
	}
	if err := viper.Unmarshal(&config); err != nil {
		panic(fmt.Errorf("unable to decode config: %s", err))
	}
	user = viper.GetString("user")
	pass = viper.GetString("pass")
	host = viper.GetString("host")
	certdir = viper.GetString("certdir")
	port = viper.GetInt("port")
	runner = query.NewBenchmarkRunner(config)
}
func main() {
	runner.Run(&query.KwdbPool, newProcessor)
}

type queryExecutorOptions struct {
	debug         bool
	printResponse bool
}

type processor struct {
	db   *commonpool.Conn
	opts *queryExecutorOptions
}

func (p *processor) Init(workerNum int) {
	db, err := commonpool.GetConnection(user, pass, host, certdir, port)
	if err != nil {
		panic(err)
	}
	p.db = db
	p.opts = &queryExecutorOptions{
		debug:         runner.DebugLevel() > 0,
		printResponse: runner.DoPrintResponses(),
	}
	ctx := context.Background()
	_, err = p.db.Connection.Exec(ctx, "set max_push_limit_number = 10000000; set can_push_sorter = true;")
	if err != nil {
		//	panic(err)
	}

}

func (p *processor) ProcessQuery(q query.Query, _ bool) ([]*query.Stat, error) {
	tq := q.(*query.Kwdb)

	start := time.Now()
	qry := string(tq.SqlQuery)
	if p.opts.debug {
		fmt.Println(qry)
	}
	querys := strings.Split(qry, ";")
	ctx := context.Background()
	//todo: 删除td结果打印逻辑，pgx库无法进行通用打印，只能一条sql，一个结构

	for i := 0; i < len(querys); i++ {
		fmt.Println(querys[i])
		rows, err := p.db.Connection.Query(ctx, querys[i])
		if err != nil {
			log.Println("Error running query: '", querys[i], "'")
			return nil, err
		}

		//var max int64
		//var timestamp time.Time
		//for rows.Next() {
		//	if err = rows.Scan(&timestamp, &max); err == nil {
		//		fmt.Printf("%d %d\n", timestamp.UTC().UnixNano(), max)
		//	} else {
		//		fmt.Printf("query error\n")
		//	}
		//}
		rows.Close()
	}
	took := float64(time.Since(start).Nanoseconds()) / 1e6
	stat := query.GetStat()
	stat.Init(q.HumanLabelName(), took)

	return []*query.Stat{stat}, nil
}

func newProcessor() query.Processor { return &processor{} }
