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

const (
	microsecFromUnixEpochToY2K = 946684800 * 1000000
	cpuPrepareColumnCount      = 12
	cpuPrepareTableName        = "cpu"
	cpuPrepareInsertPrefix     = "insert into %s.cpu (k_timestamp,usage_user,usage_system,usage_idle,usage_nice,usage_iowait,usage_irq,usage_softirq,usage_steal,usage_guest,usage_guest_nice,hostname) values "
)

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

type preparedInsertProcessor struct {
	opts               *LoadingOptions
	dbName             string
	sci                *syncCSI
	client             preparedBatchClient
	pooledConn         *commonpool.Conn
	deviceNum          int
	workerIndex        int
	executor           PreparedBatchExecutor
	preparedStatements map[string]PreparedStatementHandle
	buffer             map[string]*fixedArgList
	buffInited         bool
	formatBuf          []int16
	preparedValuesSQL  string
}

func newPreparedInsertProcessor(opts *LoadingOptions, dbName string, executor PreparedBatchExecutor) *preparedInsertProcessor {
	return &preparedInsertProcessor{
		opts:               opts,
		dbName:             dbName,
		sci:                globalSCI,
		executor:           executor,
		preparedStatements: make(map[string]PreparedStatementHandle),
		buffer:             make(map[string]*fixedArgList),
		formatBuf:          buildBinaryParameterFormatCodes(opts.Preparesize * cpuPrepareColumnCount),
		preparedValuesSQL:  buildPreparedValuesSQL(opts.Preparesize, cpuPrepareColumnCount),
	}
}

func newProcessorPrepare(opts *LoadingOptions, dbName string) *preparedInsertProcessor {
	return newPreparedInsertProcessor(opts, dbName, newStandardPrepareExecutor())
}

func buildPreparedValuesSQL(prepareSize, columnCount int) string {
	var stmt strings.Builder
	stmt.Grow(Size1M)

	for i := 0; i < prepareSize; i++ {
		stmt.WriteByte('(')
		for col := 0; col < columnCount; col++ {
			if col > 0 {
				stmt.WriteByte(',')
			}
			stmt.WriteString(fmt.Sprintf("$%d", i*columnCount+col+1))
		}
		stmt.WriteByte(')')
		if i == prepareSize-1 {
			stmt.WriteByte(';')
		} else {
			stmt.WriteByte(',')
		}
	}

	return stmt.String()
}

func buildBinaryParameterFormatCodes(count int) []int16 {
	formatBuf := make([]int16, count)
	for i := range formatBuf {
		formatBuf[i] = 1
	}
	return formatBuf
}

func (p *preparedInsertProcessor) Init(workerNum int, doLoad, _ bool) {
	if !doLoad {
		return
	}

	p.workerIndex = workerNum

	conn, err := commonpool.GetConnection(p.opts.User, p.opts.Pass, p.opts.Host, p.opts.CertDir, p.opts.Port)
	if err != nil {
		panic(err)
	}

	p.pooledConn = conn
	p.client = conn
}

func (p *preparedInsertProcessor) ProcessBatch(b targets.Batch, doLoad bool) (metricCount, rowCount uint64) {
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

	tableBuffer := p.ensurePrepareBuffer(cpuPrepareTableName, p.opts.Preparesize*cpuPrepareColumnCount)
	_, cpuPrepared := p.preparedStatements[cpuPrepareTableName]

	for _, args := range batches.m {
		rowCnt += uint64(len(args))
		for _, s := range args {
			s = s[1 : len(s)-1]
			p.parseCPURowIntoBuffer(s, tableBuffer)

			if tableBuffer.Length() == tableBuffer.Capacity() {
				if !cpuPrepared {
					p.prepareStatement(cpuPrepareTableName)
					cpuPrepared = true
				}

				p.execPreparedStatement(cpuPrepareTableName, tableBuffer.args)
				tableBuffer.Reset()
			}
		}
	}

	return metricCnt + uint64(deviceNums)*20, rowCnt + uint64(deviceNums)
}

func (p *preparedInsertProcessor) ensurePrepareBuffer(tableName string, capacity int) *fixedArgList {
	if !p.buffInited {
		if _, ok := p.buffer[tableName]; !ok {
			buffer := newFixedArgList(capacity)
			buffer.Init()
			p.buffer[tableName] = buffer
		}
		p.buffInited = true
	}

	return p.buffer[tableName]
}

func (p *preparedInsertProcessor) prepareStatementSpec(tableName string) PreparedStatementSpec {
	stmtName := "insertall" + tableName
	sql := fmt.Sprintf(cpuPrepareInsertPrefix, p.dbName) + p.preparedValuesSQL

	return PreparedStatementSpec{
		Name:                 stmtName,
		SQL:                  sql,
		RowColumnCount:       cpuPrepareColumnCount,
		ParameterFormatCodes: p.formatBuf,
	}
}

func (p *preparedInsertProcessor) prepareStatement(tableName string) {
	spec := p.prepareStatementSpec(tableName)
	handle, err := p.executor.Prepare(context.Background(), p.client, spec)
	if err != nil {
		panic(fmt.Sprintf("kwdb Prepare failed,err :%s, sql :%s", err, spec.SQL))
	}

	p.preparedStatements[tableName] = handle
}

func (p *preparedInsertProcessor) execPreparedStatement(tableName string, args [][]byte) {
	spec := p.prepareStatementSpec(tableName)
	handle := p.preparedStatements[tableName]
	err := p.executor.Exec(context.Background(), p.client, spec, handle, args)
	if err != nil {
		panic(err)
	}
}

func (p *preparedInsertProcessor) parseCPURowIntoBuffer(s string, tableBuffer *fixedArgList) {
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

func (p *preparedInsertProcessor) Close(doLoad bool) {
	if doLoad && p.pooledConn != nil {
		p.pooledConn.Put()
	}
}

func (p *preparedInsertProcessor) createDeviceAndAttribute(createSql []*point) int {
	deviceNums := 0
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
			err := p.client.Exec(ctx.c, sql)
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
		err := p.client.Exec(context.Background(), sql)
		if err != nil {
			panic(fmt.Sprintf("kwdb prepare insert data failed,err :%s", err))
		}
	}

	return deviceNums
}
