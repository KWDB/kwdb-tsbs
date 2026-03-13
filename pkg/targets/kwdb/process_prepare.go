package kwdb

import (
	"context"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"

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

type prepareExecMode uint8

const (
	prepareExecModeStandard prepareExecMode = iota
	prepareExecModeExtended
)

type prepareProcessor struct {
	opts        *LoadingOptions
	dbName      string
	sci         *syncCSI
	_db         *commonpool.Conn
	deviceNum   int
	mode        prepareExecMode
	preparedSQL map[string]struct{}
	tsLayouts   map[string]*pgconn.KWDBTSStatementDescription

	buffer     map[string]*fixedArgList // tableName -> fixedArgList
	buffInited bool
	formatBuf  []int16
}

func newProcessorPrepare(opts *LoadingOptions, dbName string, mode prepareExecMode) *prepareProcessor {
	return &prepareProcessor{
		opts:        opts,
		dbName:      dbName,
		sci:         globalSCI,
		mode:        mode,
		preparedSQL: make(map[string]struct{}),
		tsLayouts:   make(map[string]*pgconn.KWDBTSStatementDescription),
		buffer:      make(map[string]*fixedArgList),
		formatBuf:   make([]int16, opts.Preparesize*12),
	}
}

func (p *prepareProcessor) Init(workerNum int, doLoad, _ bool) {
	if !doLoad {
		return
	}

	var err error
	p._db, err = commonpool.GetConnection(p.opts.User, p.opts.Pass, p.opts.Host, p.opts.CertDir, p.opts.Port)
	if err != nil {
		panic(err)
	}

	for i := 0; i < p.opts.Preparesize*12; i++ {
		p.formatBuf[i] = 1
	}
	_ = workerNum
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

	var deviceNums int
	if p.opts.DoCreate && len(batches.createSql) != 0 {
		p.deviceNum = len(batches.createSql)
		if batches.createSql[0].sqlType != CreateTable {
			p.deviceNum--
		}
		deviceNums = p.createDeviceAndAttribute(batches.createSql)
	}

	if !p.buffInited {
		if _, ok := p.buffer["cpu"]; !ok {
			buffer := newFixedArgList(p.opts.Preparesize * 12)
			buffer.Init()
			p.buffer["cpu"] = buffer
		}
		p.buffInited = true
	}
	tableBuffer := p.buffer["cpu"]

	for _, args := range batches.m {
		rowCnt += uint64(len(args))
		for _, s := range args {
			s = s[1 : len(s)-1]
			p.parseCPURowIntoBuffer(s, tableBuffer)
			if tableBuffer.Length() == tableBuffer.Capacity() {
				p.flushTableBuffer("cpu", tableBuffer, 12)
			}
		}
	}

	return metricCnt + uint64(deviceNums)*20, rowCnt + uint64(deviceNums)
}

func (p *prepareProcessor) parseCPURowIntoBuffer(s string, tableBuffer *fixedArgList) {
	start := 0
	fieldIdx := 0
	sLen := len(s)

	for pos := 0; pos <= sLen; pos++ {
		if pos != sLen && s[pos] != ',' {
			continue
		}

		v := s[start:pos]
		if fieldIdx < 11 {
			num, ok := fastParseInt(v)
			if !ok {
				num, _ = strconv.ParseInt(v, 10, 64)
			}
			if fieldIdx == 0 {
				tableBuffer.Emplace(uint64(num*1000) - microsecFromUnixEpochToY2K)
			} else {
				tableBuffer.Emplace(uint64(num))
			}
		} else {
			left := 0
			right := len(v) - 1
			for left <= right && (v[left] == ' ' || v[left] == '\t' || v[left] == '\n' || v[left] == '\r') {
				left++
			}
			for right >= left && (v[right] == ' ' || v[right] == '\t' || v[right] == '\n' || v[right] == '\r') {
				right--
			}
			trimmed := v[left : right+1]
			q1 := strings.IndexByte(trimmed, '\'')
			if q1 < 0 {
				panic(fmt.Sprintf("kwdb invalid hostname field: %q", trimmed))
			}
			q2 := strings.IndexByte(trimmed[q1+1:], '\'')
			if q2 < 0 {
				panic(fmt.Sprintf("kwdb invalid hostname field: %q", trimmed))
			}
			tableBuffer.Append([]byte(trimmed[q1+1 : q1+1+q2]))
		}

		fieldIdx++
		start = pos + 1
	}
}

func (p *prepareProcessor) Close(doLoad bool) {
	if doLoad {
		if tableBuffer, ok := p.buffer["cpu"]; ok {
			p.flushTableBuffer("cpu", tableBuffer, 12)
		}
		p._db.Put()
	}
}

func (p *prepareProcessor) createDeviceAndAttribute(createSql []*point) int {
	var deviceNums int
	sql := fmt.Sprintf("insert into %s.cpu (hostname,region,datacenter,rack,os,arch ,team,service,service_version,service_environment) values", p.dbName)
	for _, row := range createSql {
		deviceNums++
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

func (p *prepareProcessor) flushTableBuffer(tableName string, tableBuffer *fixedArgList, colCountPerRow int) {
	if tableBuffer.Length() == 0 {
		return
	}
	if tableBuffer.Length()%colCountPerRow != 0 {
		panic(fmt.Sprintf("kwdb invalid buffered row width: len=%d colCountPerRow=%d", tableBuffer.Length(), colCountPerRow))
	}

	rowCount := tableBuffer.Length() / colCountPerRow
	args := tableBuffer.args[:tableBuffer.Length()]
	p.ensurePrepared(tableName, rowCount)

	switch p.mode {
	case prepareExecModeStandard:
		p.execPrepareStmt(tableName, rowCount, args)
	case prepareExecModeExtended:
		p.execPrepareStmtEx(tableName, args, colCountPerRow)
	default:
		panic(fmt.Sprintf("unknown prepare exec mode %d", p.mode))
	}

	tableBuffer.Reset()
}

func (p *prepareProcessor) ensurePrepared(tableName string, rowCount int) {
	switch p.mode {
	case prepareExecModeStandard:
		stmtName := p.standardStmtName(tableName, rowCount)
		if _, ok := p.preparedSQL[stmtName]; ok {
			return
		}
		sql := p.buildPrepareSQL(tableName, rowCount)
		if _, err := p._db.Connection.Prepare(context.Background(), stmtName, sql); err != nil {
			panic(fmt.Sprintf("kwdb Prepare failed,err :%s, sql :%s", err, sql))
		}
		p.preparedSQL[stmtName] = struct{}{}
	case prepareExecModeExtended:
		if _, ok := p.tsLayouts[tableName]; ok {
			return
		}
		layout, err := p._db.Connection.PrepareKWDBTS(context.Background(), p.extendedStmtName(tableName), p.kwdbTableName(tableName))
		if err != nil {
			panic(fmt.Sprintf("kwdb PrepareKWDBTS failed,err :%s, table :%s", err, p.kwdbTableName(tableName)))
		}
		p.tsLayouts[tableName] = layout
	default:
		panic(fmt.Sprintf("unknown prepare exec mode %d", p.mode))
	}
}

func (p *prepareProcessor) standardStmtName(tableName string, rowCount int) string {
	return fmt.Sprintf("insertall%s_%d", tableName, rowCount)
}

func (p *prepareProcessor) extendedStmtName(tableName string) string {
	return fmt.Sprintf("insertall%s_ts", tableName)
}

func (p *prepareProcessor) kwdbTableName(tableName string) string {
	return fmt.Sprintf("%s.public.%s", p.opts.DBName, tableName)
}

func (p *prepareProcessor) buildPrepareSQL(tableName string, rowCount int) string {
	if tableName != "cpu" {
		panic(fmt.Sprintf("unsupported prepare table %s", tableName))
	}

	var b strings.Builder
	fmt.Fprintf(&b, "insert into %s.cpu (k_timestamp,usage_user,usage_system,usage_idle,usage_nice,usage_iowait,usage_irq,usage_softirq,usage_steal,usage_guest,usage_guest_nice,hostname) values ", p.opts.DBName)
	for i := 0; i < rowCount; i++ {
		fmt.Fprintf(&b, "($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)",
			i*12+1, i*12+2, i*12+3, i*12+4, i*12+5, i*12+6,
			i*12+7, i*12+8, i*12+9, i*12+10, i*12+11, i*12+12)
		if i == rowCount-1 {
			b.WriteByte(';')
		} else {
			b.WriteByte(',')
		}
	}
	return b.String()
}

func (p *prepareProcessor) execPrepareStmt(tableName string, rowCount int, args [][]byte) {
	stmtName := p.standardStmtName(tableName, rowCount)
	res := p._db.Connection.PgConn().ExecPrepared(context.Background(), stmtName, args, p.formatBuf[:len(args)], nil).Read()
	if res.Err != nil {
		panic(res.Err)
	}
}

func (p *prepareProcessor) execPrepareStmtEx(tableName string, args [][]byte, colCountPerRow int) {
	layout := p.tsLayouts[tableName]
	res := p._db.Connection.PgConn().ExecPreparedKWDBTS(context.Background(), p.extendedStmtName(tableName), layout, args, colCountPerRow).Read()
	if res.Err != nil {
		panic(res.Err)
	}
}
