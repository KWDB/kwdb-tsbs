package kwdb

import (
	"context"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"

	"github.com/timescale/tsbs/pkg/targets"
	"github.com/timescale/tsbs/pkg/targets/kwdb/commonpool"
)

const microsecFromUnixEpochToY2K = 946684800 * 1000000

type fixedArgList struct {
	args     [][]byte
	capacity int
	writePos int
}

func newFixedArgList(capacity int) *fixedArgList {
	return &fixedArgList{
		args:     make([][]byte, capacity),
		capacity: capacity,
		writePos: 0,
	}
}

func (fa *fixedArgList) Init() {
	for i := 0; i < fa.capacity; i++ {
		fa.args[i] = make([]byte, 8)
	}
}

func (fa *fixedArgList) Reset() {
	// fa.args = fa.args[:0]
	fa.writePos = 0
}

func (fa *fixedArgList) Append(value []byte) {
	fa.args[fa.writePos] = value
	fa.writePos++
}

func (fa *fixedArgList) Emplace(value uint64) {
	binary.BigEndian.PutUint64(fa.args[fa.writePos], value)
	fa.writePos++
}

func (fa *fixedArgList) Capacity() int {
	return fa.capacity
}

func (fa *fixedArgList) Length() int {
	return fa.writePos
}

type prepareProcessor struct {
	opts        *LoadingOptions
	dbName      string
	sci         *syncCSI
	_db         *commonpool.Conn
	deviceNum   int
	preparedSql map[string]struct{}
	prepareStmt strings.Builder
	workerIndex int

	// prepare buff
	buffer     map[string]*fixedArgList // tableName, fixedArgList
	buffInited bool
	formatBuf  []int16
}

func newProcessorPrepare(opts *LoadingOptions, dbName string) *prepareProcessor {
	return &prepareProcessor{
		opts:        opts,
		dbName:      dbName,
		sci:         globalSCI,
		preparedSql: make(map[string]struct{}),
		buffer:      make(map[string]*fixedArgList),
		formatBuf:   make([]int16, opts.Preparesize*12),
	}
}

func (p *prepareProcessor) Init(workerNum int, doLoad, _ bool) {
	if !doLoad {
		return
	}

	p.prepareStmt.Grow(Size1M)

	for i := 0; i < p.opts.Preparesize; i++ {
		p.prepareStmt.WriteString(fmt.Sprintf("($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)",
			i*12+1, i*12+2, i*12+3, i*12+4, i*12+5, i*12+6, i*12+7, i*12+8, i*12+9, i*12+10, i*12+11,
			i*12+12))
		if i == p.opts.Preparesize-1 {
			p.prepareStmt.WriteString(";")
		} else {
			p.prepareStmt.WriteString(",")
		}
	}

	p.workerIndex = workerNum

	var err error
	p._db, err = commonpool.GetConnection(p.opts.User, p.opts.Pass, p.opts.Host, p.opts.CertDir, p.opts.Port)
	if err != nil {
		panic(err)
	}

	for i := 0; i < p.opts.Preparesize*12; i++ {
		p.formatBuf[i] = 1
	}
}

func (p *prepareProcessor) ProcessBatch(b targets.Batch, doLoad bool) (metricCount, rowCount uint64) {
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
	var deviceNums int
	if p.opts.DoCreate && len(batches.createSql) != 0 {
		p.deviceNum = len(batches.createSql)
		if batches.createSql[0].sqlType != CreateTable {
			p.deviceNum--
		}
		deviceNums = p.createDeviceAndAttribute(batches.createSql)
	} else {
		deviceNums = 0
	}

	// init buffer for every table
	if !p.buffInited {
		_, ok := p.buffer["cpu"]
		if !ok {
			buffer := newFixedArgList(p.opts.Preparesize * 12)
			buffer.Init()
			p.buffer["cpu"] = buffer
		}
		p.buffInited = true
	}

	// join args and execute
	for _, args := range batches.m {
		rowCnt += uint64(len(args))
		tableBuffer := p.buffer["cpu"]

		for _, s := range args {
			s = s[1 : len(s)-1]
			values := strings.Split(s, ",")

			// Emplace
			for i, v := range values {
				if i < 11 {
					num, ok := fastParseInt(v)
					if !ok {
						num, _ = strconv.ParseInt(v, 10, 64)
					}
					if i == 0 {
						// timestamp: UTC+8 Time Zone
						tableBuffer.Emplace(uint64(num*1000) - microsecFromUnixEpochToY2K)
					} else {
						// row data
						tableBuffer.Emplace(uint64(num))
					}
				} else {
					v = strings.TrimSpace(v)
					vv := strings.Split(v, "'")
					tableBuffer.Append([]byte(vv[1]))
				}
			}

			// check buffer is full
			if tableBuffer.Length() == tableBuffer.Capacity() {
				// init prepareStmt
				_, ok := p.preparedSql["cpu"]
				if !ok {
					p.createPrepareSql("cpu")
					p.preparedSql["cpu"] = struct{}{}
				}

				p.execPrepareStmt("cpu", tableBuffer.args)
				// reuse buffer: reset tableBuffer's write position
				tableBuffer.Reset()
			}
		}
	}

	// batches.Reset()
	return metricCnt + uint64(deviceNums)*20, rowCnt + uint64(deviceNums)
}

func (p *prepareProcessor) Close(doLoad bool) {
	if doLoad {
		p._db.Put()
	}
}

func (p *prepareProcessor) createDeviceAndAttribute(createSql []*point) int {
	var deviceNums int = 0
	sql := fmt.Sprintf("insert into %s.cpu (hostname,region,datacenter,rack,os,arch ,team,service,service_version,service_environment) values", p.dbName)
	for _, row := range createSql {
		deviceNums += 1
		switch row.sqlType {
		case CreateTemplateTable:
			c, cancel := context.WithCancel(context.Background())
			ctx := &Ctx{
				c:      c,
				cancel: cancel,
			}
			actual, _ := p.sci.m.LoadOrStore(row.template, ctx)
			sql := fmt.Sprintf("create table %s.%s %s", p.opts.DBName, row.template, row.sql)
			_, err := p._db.Connection.Exec(ctx.c, sql)
			if err != nil {
				panic(fmt.Sprintf("kwdb create device failed,err :%s", err))
			}

			if err != nil && !strings.Contains(err.Error(), "already exists") {
				panic(fmt.Sprintf("kwdb create device failed,err :%s", err))
			}
			actual.(*Ctx).cancel()
		case CreateTable:
			sql += row.sql + ","
			continue

		default:
			panic("impossible")
		}
	}
	if createSql != nil {
		sql = sql[:len(sql)-1]
		_, err := p._db.Connection.Exec(context.Background(), sql)
		if err != nil {
			panic(fmt.Sprintf("kwdb prepare insert data failed,err :%s", err))
		}
	}

	return deviceNums
}

func (p *prepareProcessor) createPrepareSql(deviecName string) {
	var insertsql strings.Builder
	query := fmt.Sprintf("insert into %s.cpu (k_timestamp,usage_user,usage_system,usage_idle,usage_nice,usage_iowait,usage_irq,usage_softirq,usage_steal,usage_guest,usage_guest_nice,hostname) values ", p.opts.DBName)
	insertsql.WriteString(query)
	sql := insertsql.String() + p.prepareStmt.String()
	_, err1 := p._db.Connection.Prepare(context.Background(), "insertall"+deviecName, sql)
	if err1 != nil {
		panic(fmt.Sprintf("kwdb Prepare failed,err :%s, sql :%s", err1, sql))
	}
}

func (p *prepareProcessor) execPrepareStmt(tableName string, args [][]byte) {
	res := p._db.Connection.PgConn().ExecPrepared(context.Background(), "insertall"+tableName, args, p.formatBuf, []int16{}).Read()
	if res.Err != nil {
		panic(res.Err)
	}
}
