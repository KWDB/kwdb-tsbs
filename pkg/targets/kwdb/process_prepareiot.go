package kwdb

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/timescale/tsbs/pkg/targets"
	"github.com/timescale/tsbs/pkg/targets/kwdb/commonpool"
)

func (fa *fixedArgList) EmplaceFloat64(value float64) {
	fa.Emplace(math.Float64bits(value))
}

func (fa *fixedArgList) EmplaceString(value string) {
	fa.args[fa.writePos] = []byte(value)
	fa.writePos++
}

type prepareProcessoriot struct {
	opts                   *LoadingOptions
	dbName                 string
	sci                    *syncCSI
	_db                    *commonpool.Conn
	deviceNum              int
	preparedSql            map[string]struct{}
	prepareStmt            strings.Builder
	prepareStmtReadings    strings.Builder
	prepareStmtDiagnostics strings.Builder

	workerIndex int

	// prepare buff
	buffer               map[string]*fixedArgList // tableName, fixedArgList
	buffInited           bool
	formatBuf            []int16
	formatBufReadings    []int16
	formatBufDiagnostics []int16
}

func newProcessorPrepareiot(opts *LoadingOptions, dbName string) *prepareProcessoriot {
	return &prepareProcessoriot{
		opts:                 opts,
		dbName:               dbName,
		sci:                  globalSCI,
		preparedSql:          make(map[string]struct{}),
		buffer:               make(map[string]*fixedArgList),
		formatBuf:            make([]int16, opts.Preparesize*11),
		formatBufReadings:    make([]int16, opts.Preparesize*8),
		formatBufDiagnostics: make([]int16, opts.Preparesize*4),
	}
}

func (p *prepareProcessoriot) Init(workerNum int, doLoad, _ bool) {
	if !doLoad {
		return
	}

	p.prepareStmt.Grow(Size1M)
	for i := 0; i < p.opts.Preparesize; i++ {
		// p.prepareStmt.WriteString(fmt.Sprintf("($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)", i*11+1, i*11+2, i*11+3, i*11+4, i*11+5, i*11+6, i*11+7, i*11+8, i*11+9, i*11+10, i*11+11))
		p.prepareStmt.WriteString(fmt.Sprintf("($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)", i*16+1, i*16+2, i*16+3, i*16+4, i*16+5, i*16+6, i*16+7, i*16+8, i*16+9, i*16+10, i*16+11, i*16+12, i*16+13, i*16+14, i*16+15, i*16+16))
		if i == p.opts.Preparesize-1 {
			p.prepareStmt.WriteString(";")
		} else {
			p.prepareStmt.WriteString(",")
		}
	}
	p.prepareStmtReadings.Grow(Size1M)
	for i := 0; i < p.opts.Preparesize; i++ {
		// p.prepareStmt.WriteString(fmt.Sprintf("($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)", i*11+1, i*11+2, i*11+3, i*11+4, i*11+5, i*11+6, i*11+7, i*11+8, i*11+9, i*11+10, i*11+11))
		p.prepareStmtReadings.WriteString(fmt.Sprintf("($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)", i*8+1, i*8+2, i*8+3, i*8+4, i*8+5, i*8+6, i*8+7, i*8+8))
		if i == p.opts.Preparesize-1 {
			p.prepareStmtReadings.WriteString(";")
		} else {
			p.prepareStmtReadings.WriteString(",")
		}
	}
	p.prepareStmtDiagnostics.Grow(Size1M)
	for i := 0; i < p.opts.Preparesize; i++ {
		p.prepareStmtDiagnostics.WriteString(fmt.Sprintf("($%d,$%d,$%d,$%d)", i*4+1, i*4+2, i*4+3, i*4+4))
		if i == p.opts.Preparesize-1 {
			p.prepareStmtDiagnostics.WriteString(";")
		} else {
			p.prepareStmtDiagnostics.WriteString(",")
		}
	}
	p.workerIndex = workerNum

	var err error
	p._db, err = commonpool.GetConnection(p.opts.User, p.opts.Pass, p.opts.Host, p.opts.CertDir, p.opts.Port)
	if err != nil {
		panic(err)
	}

	for i := 0; i < p.opts.Preparesize*11; i++ {
		p.formatBuf[i] = 1
	}

	for i := 0; i < p.opts.Preparesize*8; i++ {
		p.formatBufReadings[i] = 1
	}

	for i := 0; i < p.opts.Preparesize*4; i++ {
		p.formatBufDiagnostics[i] = 1
	}
}

func (p *prepareProcessoriot) ProcessBatch(b targets.Batch, doLoad bool) (metricCount, rowCount uint64) {
	batches := b.(*hypertableArr)
	rowCnt := uint64(0)
	metricCnt := batches.totalMetric
	if !doLoad {
		for _, sqls := range batches.m {
			rowCnt += uint64(len(sqls))
		}
		return metricCnt, rowCnt
	}

	// create table
	if p.opts.DoCreate && len(batches.createSql) != 0 {
		p.deviceNum = len(batches.createSql)
		if batches.createSql[0].sqlType != CreateTable {
			p.deviceNum--
		}
		p.createDeviceAndAttribute(batches.createSql)
	}

	// init buffer for every table
	if !p.buffInited {
		for tableName := range batches.m {
			_, ok := p.buffer[tableName]
			if !ok {
				buffer := newFixedArgList(p.opts.Preparesize * getPreparesize(tableName))
				buffer.Init()
				p.buffer[tableName] = buffer
			}
		}
		p.buffInited = true
	}

	// join args and execute
	for tableName, args := range batches.m {
		rowCnt += uint64(len(args))
		tableBuffer := p.buffer[tableName]

		for _, s := range args {
			s = s[1 : len(s)-1]
			values := strings.Split(s, ",")

			// Emplace
			for i, v := range values {
				if i == 0 {
					// timestamp: UTC+8 Time Zone
					num, _ := strconv.ParseInt(v, 10, 64)
					tableBuffer.Emplace(uint64(num*1000) - microsecFromUnixEpochToY2K + 8*3600*1000000)
				} else {
					// row data
					if num, err := strconv.ParseInt(v, 10, 64); err == nil {
						tableBuffer.Emplace(uint64(num))
						continue
					}
					if num, err := strconv.ParseFloat(v, 64); err == nil {
						tableBuffer.EmplaceFloat64(num)
						continue
					}
					tableBuffer.EmplaceString(v)
				}
			}

			// check buffer is full
			if tableBuffer.Length() == tableBuffer.Capacity() {
				// init prepareStmt
				_, ok := p.preparedSql[tableName]
				if !ok {
					p.createPrepareSql(tableName)
					p.preparedSql[tableName] = struct{}{}
				}

				p.execPrepareStmt(tableName, tableBuffer.args)
				// reuse buffer: reset tableBuffer's write position
				tableBuffer.Reset()
			}
		}
	}

	// batches.Reset()
	return metricCnt, rowCnt
}

// 根据 tableName 获取相应的 preparesize 值
func getPreparesize(tableName string) int {
	if strings.HasPrefix(tableName, "readings") {
		return 8
	} else if strings.HasPrefix(tableName, "diagnostics") {
		return 4
	}
	return -1
}

func (p *prepareProcessoriot) Close(doLoad bool) {
	if doLoad {
		p._db.Put()
	}
}

func (p *prepareProcessoriot) createDeviceAndAttribute(createSql []*point) {
	for _, row := range createSql {
		switch row.sqlType {
		case CreateTemplateTable:
			c, cancel := context.WithCancel(context.Background())
			ctx := &Ctx{
				c:      c,
				cancel: cancel,
			}
			actual, _ := p.sci.m.LoadOrStore(row.template, ctx)
			sql := fmt.Sprintf("create table %s.%s %s", p.opts.DBName, row.template, row.sql)
			//fmt.Println(sql)
			_, err := p._db.Connection.Exec(ctx.c, sql)
			if err != nil {
				panic(fmt.Sprintf("kwdb create device failed,err :%s", err))
			}

			if err != nil && !strings.Contains(err.Error(), "already exists") {
				panic(fmt.Sprintf("kwdb create device failed,err :%s", err))
			}
			actual.(*Ctx).cancel()
		case CreateTable:
			c, cancel := context.WithCancel(context.Background())
			ctx := &Ctx{
				c:      c,
				cancel: cancel,
			}
			actual, _ := p.sci.m.LoadOrStore(row.device, ctx)

			v, ok := p.sci.m.Load(row.template)
			if ok {
				<-v.(*Ctx).c.Done() //等待v.(*Ctx).c的上下文完成。
				sql := fmt.Sprintf("create table %s.%s using %s %s", p.opts.DBName, row.device, row.template, row.sql)
				//fmt.Println(sql)
				_, err := p._db.Connection.Exec(ctx.c, sql)
				if err != nil {
					panic(fmt.Sprintf(" 249:kwdb create device failed,err :%s, sql :%s, ", err, sql))
				}
				if err != nil && !strings.Contains(err.Error(), "already exists") {
					fmt.Println(sql)
					panic(err)
				}

				actual.(*Ctx).cancel()
				continue
			}
			// wait for template table created
			templateTableC, templateTableCancel := context.WithCancel(context.Background())
			templateTableCtx := &Ctx{
				c:      templateTableC,
				cancel: templateTableCancel,
			}
			templateTableActual, _ := p.sci.m.LoadOrStore(row.template, templateTableCtx)
			<-templateTableActual.(*Ctx).c.Done()

			sql := fmt.Sprintf("create table %s.%s using %s %s", p.opts.DBName, row.device, row.template, row.sql)
			//fmt.Println(sql)
			_, err := p._db.Connection.Exec(ctx.c, sql)
			if err != nil {
				panic(fmt.Sprintf("284:create table failed because %s", err.Error()))
			}
			if err != nil && !strings.Contains(err.Error(), "already exists") {
				fmt.Println(sql)
				panic(err)
			}

			actual.(*Ctx).cancel()
		default:
			panic("impossible")
		}
	}
}

func (p *prepareProcessoriot) createPrepareSql(deviecName string) {
	var insertsql strings.Builder
	query := fmt.Sprintf("insert into %s.%s values ", p.opts.DBName, deviecName)
	insertsql.WriteString(query)
	if strings.HasPrefix(deviecName, "readings") {
		sql := insertsql.String() + p.prepareStmtReadings.String()
		_, err1 := p._db.Connection.Prepare(context.Background(), "insertall"+deviecName, sql)
		if err1 != nil {
			panic(fmt.Sprintf("265:kwdb Prepare failed,err :%s, sql :%s", err1, sql))
		}
	} else if strings.HasPrefix(deviecName, "diagnostics") {
		sql := insertsql.String() + p.prepareStmtDiagnostics.String()
		_, err1 := p._db.Connection.Prepare(context.Background(), "insertall"+deviecName, sql)
		if err1 != nil {
			panic(fmt.Sprintf("265:kwdb Prepare failed,err :%s, sql :%s", err1, sql))
		}
	} else {
		panic(fmt.Sprintf("unknown table %s", deviecName))
	}
}

func (p *prepareProcessoriot) execPrepareStmt(tableName string, args [][]byte) {
	if strings.HasPrefix(tableName, "readings") {
		res := p._db.Connection.PgConn().ExecPrepared(context.Background(), "insertall"+tableName, args, p.formatBufReadings, []int16{}).Read()
		if res.Err != nil {
			panic(res.Err)
		}
	} else if strings.HasPrefix(tableName, "diagnostics") {
		res := p._db.Connection.PgConn().ExecPrepared(context.Background(), "insertall"+tableName, args, p.formatBufDiagnostics, []int16{}).Read()
		if res.Err != nil {
			panic(res.Err)
		}
	} else {
		fmt.Printf("unknown table %s\n", tableName)
	}
}
