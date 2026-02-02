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
	user      string
	pass      string
	host      string
	certdir   string
	querytype string
	port      int
	runner    *query.BenchmarkRunner
	prepare   bool
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
	querytype = viper.GetString("query-type")
	prepare = viper.GetBool("prepare")
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

	prepareStmt strings.Builder
	formatBuf   []int16
	buffer      map[string]*fixedArgList
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
	// 此配置用于打开time_bucket+聚合计算的SQL语句的下推计算功能.范围是sessions级别的，只针对于当前窗口
	_, err = p.db.Connection.Exec(ctx, "set enable_timebucket_opt = true;")
	if err != nil {
		//	panic(err)
	}
	if prepare {
		// 查询模板初始化
		p.Initquery(querytype)
		sql := p.prepareStmt.String()
		_, err1 := p.db.Connection.Prepare(ctx, querytype, sql)
		if err1 != nil {
			panic(fmt.Sprintf("%s Prepare failed,err :%s, sql :%s", querytype, err1, sql))
		}
	}

}

func (p *processor) ProcessQuery(q query.Query, prepare bool) ([]*query.Stat, error) {
	tq := q.(*query.Kwdb)

	if tq.Querytype != querytype {
		panic(fmt.Sprintf("The specified query type \"%s\" is inconsistent with the query file type \"%s\"", querytype, tq.Querytype))
	}
	start := time.Now()
	qry := string(tq.SqlQuery)
	if p.opts.debug {
		fmt.Println(qry)
	}
	querys := strings.Split(qry, ";")
	ctx := context.Background()

	for i := 0; i < len(querys); i++ {
		if !prepare {
			//fmt.Println(querys[i])
			rows, err := p.db.Connection.Query(ctx, querys[i])
			if err != nil {
				log.Println("Error running query: '", querys[i], "'")
				return nil, err
			}
			//for rows.Next() {
			//	values, err := rows.Values()
			//	if err != nil {
			//		return nil, err
			//	}
			//	fmt.Println(values)
			//}

			rows.Close()
		} else {
			fmt.Println(querys)

			tableBuffer := p.buffer[tq.Querytype]
			p.RunSelect(tq.Querytype, strings.Split(qry, ","), tableBuffer)
			res := p.db.Connection.PgConn().ExecPrepared(ctx, tq.Querytype, tableBuffer.args, p.formatBuf, []int16{}).Read()
			if res.Err != nil {
				panic(res.Err)
			}
			tableBuffer.Reset()
		}
	}
	took := float64(time.Since(start).Nanoseconds()) / 1e6
	stat := query.GetStat()
	stat.Init(q.HumanLabelName(), took)

	return []*query.Stat{stat}, nil
}

func newProcessor() query.Processor { return &processor{} }
