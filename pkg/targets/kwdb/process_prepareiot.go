package kwdb

import (
	"context"
	"fmt"
	"github.com/timescale/tsbs/pkg/targets"
	"github.com/timescale/tsbs/pkg/targets/kwdb/commonpool"
	"math"
	"strconv"
	"strings"
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
	buffer               map[string]*fixedArgList
	buffInited           bool
	formatBufReadings    []int16
	formatBufDiagnostics []int16
	tables               map[string]string
}

func newProcessorPrepareiot(opts *LoadingOptions, dbName string) *prepareProcessoriot {
	return &prepareProcessoriot{
		opts:                 opts,
		dbName:               dbName,
		sci:                  globalSCI,
		preparedSql:          make(map[string]struct{}),
		buffer:               make(map[string]*fixedArgList),
		formatBufReadings:    make([]int16, opts.Preparesize*9),
		formatBufDiagnostics: make([]int16, opts.Preparesize*5),
		tables: map[string]string{
			"readings": fmt.Sprintf("insert into %s.readings (name, fleet, driver, model, "+
				"device_version, load_capacity, fuel_capacity, nominal_fuel_consumption) values", opts.DBName),
			"diagnostics": fmt.Sprintf("insert into %s.diagnostics (name, fleet, driver, model, "+
				"device_version, load_capacity, fuel_capacity, nominal_fuel_consumption) values", opts.DBName),
		},
	}
}

func (p *prepareProcessoriot) Init(workerNum int, doLoad, _ bool) {
	if !doLoad {
		return
	}
	p.prepareStmtReadings.Grow(Size1M)
	for i := 0; i < p.opts.Preparesize; i++ {
		p.prepareStmtReadings.WriteString(fmt.Sprintf("($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)",
			i*9+1, i*9+2, i*9+3, i*9+4, i*9+5, i*9+6, i*9+7, i*9+8, i*9+9))
		p.prepareStmtReadings.WriteString(",")
	}
	p.prepareStmtReadings.WriteString(";")
	p.prepareStmtDiagnostics.Grow(Size1M)
	for i := 0; i < p.opts.Preparesize; i++ {
		p.prepareStmtDiagnostics.WriteString(fmt.Sprintf("($%d,$%d,$%d,$%d,$%d)",
			i*5+1, i*5+2, i*5+3, i*5+4, i*5+5))
		p.prepareStmtDiagnostics.WriteString(",")
	}
	p.prepareStmtDiagnostics.WriteString(";")
	p.workerIndex = workerNum

	var err error
	p._db, err = commonpool.GetConnection(p.opts.User, p.opts.Pass, p.opts.Host, p.opts.CertDir, p.opts.Port)
	if err != nil {
		panic(err)
	}

	for i := 0; i < p.opts.Preparesize*9; i++ {
		p.formatBufReadings[i] = 1
	}

	for i := 0; i < p.opts.Preparesize*5; i++ {
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

	for tableName, args := range batches.m {
		isReadings := tableName[:8] == "readings"
		tableType := "readings"
		if !isReadings {
			tableType = "diagnostics"
		}

		_, ok := p.buffer[tableType]
		if !ok {
			buffer := newFixedArgList(p.opts.Preparesize * getPreparesize(tableName))
			buffer.Init()
			p.buffer[tableType] = buffer
		}

		rowCnt += uint64(len(args))
		tableBuffer := p.buffer[tableType]

		for _, s := range args {
			s = s[1 : len(s)-1]
			start := 0
			i := 0 // 字段索引

			if tableType == "readings" {
				for pos, char := range s {
					if char == ',' || pos == len(s)-1 {
						end := pos
						if pos == len(s)-1 {
							end = len(s)
						}
						v := s[start:end]

						if i == 0 { // 时间戳
							num, _ := strconv.ParseInt(v, 10, 64)
							tableBuffer.Emplace(uint64(num*1000) - microsecFromUnixEpochToY2K + 8*3600*1000000)
						} else if i == 8 {
							v = strings.TrimSpace(v)
							vv := strings.Split(v, "'")
							tableBuffer.Append([]byte(vv[1]))
						} else {
							num, _ := strconv.ParseFloat(v, 64)
							tableBuffer.EmplaceFloat64(num)
						}

						start = pos + 1
						i++
					}
				}
			} else {
				for pos, char := range s {
					if char == ',' || pos == len(s)-1 {
						end := pos
						if pos == len(s)-1 {
							end = len(s)
						}
						v := s[start:end]

						// 处理每个字段
						if i == 0 {
							num, _ := strconv.ParseInt(v, 10, 64)
							tableBuffer.Emplace(uint64(num*1000) - microsecFromUnixEpochToY2K + 8*3600*1000000)
						} else if i == 4 {
							v = strings.TrimSpace(v)
							vv := strings.Split(v, "'")
							tableBuffer.Append([]byte(vv[1]))
						} else if i == 3 {
							if num, err := strconv.ParseInt(v, 10, 64); err == nil {
								tableBuffer.Emplace(uint64(num))
							}
						} else {
							if num, err := strconv.ParseFloat(v, 64); err == nil {
								tableBuffer.EmplaceFloat64(num)
							}
						}

						start = pos + 1
						i++
					}
				}
			}
			// check buffer is full
			if tableBuffer.Length() == tableBuffer.Capacity() {
				_, ok := p.preparedSql[tableType]
				if !ok {
					p.createPrepareSql(tableType)
					p.preparedSql[tableType] = struct{}{}
				}
				p.execPrepareStmt(tableType, tableBuffer.args)
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
		return 9
	} else if strings.HasPrefix(tableName, "diagnostics") {
		return 5
	}
	return -1
}

func (p *prepareProcessoriot) Close(doLoad bool) {
	if doLoad {
		p._db.Put()
	}
}

const (
	Len_reading     = 141
	Len_diagnostics = 138
)

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
			_, err := p._db.Connection.Exec(ctx.c, sql)
			if err != nil && !strings.Contains(err.Error(), "already exists") {
				panic(fmt.Sprintf("kwdb create device failed, err: %s", err))
			}
			actual.(*Ctx).cancel()

		case CreateTable:
			if sql, ok := p.tables[row.template]; ok {
				p.tables[row.template] = sql + row.sql + ","
			}
			continue

		default:
			panic("impossible")
		}
	}

	for tableName, sql := range p.tables {
		if len(sql) != Len_diagnostics && len(sql) != Len_reading && len(sql) > 0 {
			sql = sql[:len(sql)-1]
			_, err := p._db.Connection.Exec(context.Background(), sql)
			if err != nil {
				panic(fmt.Sprintf("kwdb prepare insert data failed for %s, err: %s", tableName, err))
			}
		}
	}
}

func (p *prepareProcessoriot) createPrepareSql(deviecName string) {
	var insertsql strings.Builder
	if strings.HasPrefix(deviecName, "readings") {
		query := fmt.Sprintf("insert into %s.readings (k_timestamp,latitude,longitude,elevation,velocity,heading,grade,fuel_consumption,name) values ", p.opts.DBName)
		insertsql.WriteString(query)
		sql := insertsql.String() + p.prepareStmtReadings.String()
		_, err1 := p._db.Connection.Prepare(context.Background(), "insertallreadings", sql)
		if err1 != nil {
			panic(fmt.Sprintf("265:kwdb Prepare failed,err :%s, sql :%s", err1, sql))
		}
	} else if strings.HasPrefix(deviecName, "diagnostics") {
		query := fmt.Sprintf("insert into %s.diagnostics (k_timestamp,fuel_state,current_load,status,name) values ", p.opts.DBName)
		insertsql.WriteString(query)
		sql := insertsql.String() + p.prepareStmtDiagnostics.String()
		_, err1 := p._db.Connection.Prepare(context.Background(), "insertalldiagnostics", sql)
		if err1 != nil {
			panic(fmt.Sprintf("265:kwdb Prepare failed,err :%s, sql :%s", err1, sql))
		}
	} else {
		panic(fmt.Sprintf("unknown table %s", deviecName))
	}
}

func (p *prepareProcessoriot) execPrepareStmt(tableName string, args [][]byte) {
	if tableName == "readings" {
		res := p._db.Connection.PgConn().ExecPrepared(context.Background(), "insertallreadings", args, p.formatBufReadings, []int16{}).Read()
		if res.Err != nil {
			panic(res.Err)
		}
	} else {
		res := p._db.Connection.PgConn().ExecPrepared(context.Background(), "insertalldiagnostics", args, p.formatBufDiagnostics, []int16{}).Read()
		if res.Err != nil {
			panic(res.Err)
		}
	}
}
