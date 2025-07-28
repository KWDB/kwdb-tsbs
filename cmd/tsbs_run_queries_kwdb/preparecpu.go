package main

import (
	"encoding/binary"
	"strconv"
	"strings"
)

const microsecFromUnixEpochToY2K = 946684800 * 1000000

// 查询类型
const (
	QueryTypeCPUMaxAll1          = "cpu-max-all-1"
	QueryTypeCPUMaxAll8          = "cpu-max-all-8"
	QueryTypeDoubleGroupby1      = "double-groupby-1"
	QueryTypeDoubleGroupby5      = "double-groupby-5"
	QueryTypeDoubleGroupbyAll    = "double-groupby-all"
	QueryTypeGroupbyOrderbyLimit = "groupby-orderby-limit"
	QueryTypeHighCPU1            = "high-cpu-1"
	QueryTypeHighCPUAll          = "high-cpu-all"
	QueryTypeLastPoint           = "lastpoint"
	QueryTypeSingleGroupby1_1_1  = "single-groupby-1-1-1"
	QueryTypeSingleGroupby1_1_12 = "single-groupby-1-1-12"
	QueryTypeSingleGroupby1_8_1  = "single-groupby-1-8-1"
	QueryTypeSingleGroupby5_1_1  = "single-groupby-5-1-1"
	QueryTypeSingleGroupby5_1_12 = "single-groupby-5-1-12"
	QueryTypeSingleGroupby5_8_1  = "single-groupby-5-8-1"
)

// 用于prepare查询时的参数缓存、数据管理和写入控制
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

func (fa *fixedArgList) Emplace(value uint64) {
	binary.BigEndian.PutUint64(fa.args[fa.writePos], value)
	fa.writePos++
}
func (fa *fixedArgList) Append(value []byte) {
	fa.args[fa.writePos] = value
	fa.writePos++
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

func (p *processor) Initquery(s string) {
	switch s {
	// cpu-only
	case QueryTypeCPUMaxAll1:
		p.InitCpu1()
		buffer := newFixedArgList(3)
		buffer.Init()
		p.buffer = make(map[string]*fixedArgList)
		p.buffer[s] = buffer
	case QueryTypeCPUMaxAll8:
		p.InitCpu8()
		buffer := newFixedArgList(10)
		buffer.Init()
		p.buffer = make(map[string]*fixedArgList)
		p.buffer[s] = buffer
	case QueryTypeDoubleGroupby1, QueryTypeDoubleGroupby5, QueryTypeDoubleGroupbyAll:
		p.InitDoubleGroupby()
		buffer := newFixedArgList(2)
		buffer.Init()
		p.buffer = make(map[string]*fixedArgList)
		p.buffer[s] = buffer
	case QueryTypeGroupbyOrderbyLimit:
		p.InitGroupbyOrder()
		buffer := newFixedArgList(1)
		buffer.Init()
		p.buffer = make(map[string]*fixedArgList)
		p.buffer[s] = buffer
	case QueryTypeHighCPU1:
		p.InitHighCpu1()
		buffer := newFixedArgList(3)
		buffer.Init()
		p.buffer = make(map[string]*fixedArgList)
		p.buffer[s] = buffer
	case QueryTypeHighCPUAll:
		p.InitHighCpuall()
		buffer := newFixedArgList(2)
		buffer.Init()
		p.buffer = make(map[string]*fixedArgList)
		p.buffer[s] = buffer
	case QueryTypeLastPoint:
		p.InitLastPoint()
		buffer := newFixedArgList(0)
		buffer.Init()
		p.buffer = make(map[string]*fixedArgList)
		p.buffer[s] = buffer
	case QueryTypeSingleGroupby1_1_1, QueryTypeSingleGroupby1_1_12,
		QueryTypeSingleGroupby5_1_1, QueryTypeSingleGroupby5_1_12:
		p.InitSingleGroupby_Host1()
		buffer := newFixedArgList(3)
		buffer.Init()
		p.buffer = make(map[string]*fixedArgList)
		p.buffer[s] = buffer
	case QueryTypeSingleGroupby1_8_1, QueryTypeSingleGroupby5_8_1:
		p.InitSingleGroupby_Hosts()
		buffer := newFixedArgList(10)
		buffer.Init()
		p.buffer = make(map[string]*fixedArgList)
		p.buffer[s] = buffer
	// iot
	case QueryTypeLastLoc:
		p.InitLastLoc()
		buffer := newFixedArgList(1)
		buffer.Init()
		p.buffer = make(map[string]*fixedArgList)
		p.buffer[s] = buffer
	case QueryTypeSingleLastLoc:
		p.InitSingleLastLoc()
		buffer := newFixedArgList(1)
		buffer.Init()
		p.buffer = make(map[string]*fixedArgList)
		p.buffer[s] = buffer
	case QueryTypeLowFuel:
		p.InitSingleLastLoc()
		buffer := newFixedArgList(1)
		buffer.Init()
		p.buffer = make(map[string]*fixedArgList)
		p.buffer[s] = buffer
	case QueryTypeHighLoad:
		p.InitHighLoad()
		buffer := newFixedArgList(1)
		buffer.Init()
		p.buffer = make(map[string]*fixedArgList)
		p.buffer[s] = buffer
	case QueryTypeStationaryTrucks:
		p.InitStationaryTrucks()
		buffer := newFixedArgList(3)
		buffer.Init()
		p.buffer = make(map[string]*fixedArgList)
		p.buffer[s] = buffer
	case QueryTypeLongDrivingSessions:
		p.InitLongDrivingSessions()
		buffer := newFixedArgList(3)
		buffer.Init()
		p.buffer = make(map[string]*fixedArgList)
		p.buffer[s] = buffer
	case QueryTypeLongDailySessions:
		p.InitLongDailySessions()
		buffer := newFixedArgList(3)
		buffer.Init()
		p.buffer = make(map[string]*fixedArgList)
		p.buffer[s] = buffer
	case QueryTypeAvgVsProjFuelConsumption:
		p.InitConsumption()
		buffer := newFixedArgList(0)
		buffer.Init()
		p.buffer = make(map[string]*fixedArgList)
		p.buffer[s] = buffer
	case QueryTypeAvgDailyDrivingDuration:
		p.InitDuration()
		buffer := newFixedArgList(2)
		buffer.Init()
		p.buffer = make(map[string]*fixedArgList)
		p.buffer[s] = buffer
	case QueryTypeAvgDailyDrivingSession:
		p.InitSession()
		buffer := newFixedArgList(2)
		buffer.Init()
		p.buffer = make(map[string]*fixedArgList)
		p.buffer[s] = buffer
	case QueryTypeAvgLoad:
		p.InitLoad()
		buffer := newFixedArgList(0)
		buffer.Init()
		p.buffer = make(map[string]*fixedArgList)
		p.buffer[s] = buffer
	case QueryTypeDailyActivity:
		p.InitActivity()
		buffer := newFixedArgList(2)
		buffer.Init()
		p.buffer = make(map[string]*fixedArgList)
		p.buffer[s] = buffer
	case QueryTypeBreakdownFrequency:
		p.InitFrequency()
		buffer := newFixedArgList(2)
		buffer.Init()
		p.buffer = make(map[string]*fixedArgList)
		p.buffer[s] = buffer
	}
}

// cpu-max-all-1
func (p *processor) InitCpu1() {
	p.prepareStmt.Grow(350)
	p.prepareStmt.WriteString("SELECT time_bucket(k_timestamp, '3600s') as k_timestamp, max(usage_user), " +
		"max(usage_system), max(usage_idle), max(usage_nice), max(usage_iowait), max(usage_irq), " +
		"max(usage_softirq), max(usage_steal), max(usage_guest), max(usage_guest_nice) FROM benchmark.cpu " +
		"WHERE hostname = $1 AND k_timestamp >= $2 AND k_timestamp < $3 " +
		"GROUP BY time_bucket(k_timestamp, '3600s') ORDER BY time_bucket(k_timestamp, '3600s')")
	p.formatBuf = make([]int16, 3)
	for i := 0; i < 3; i++ {
		p.formatBuf[i] = 1
	}
}

// cpu-max-all-8
func (p *processor) InitCpu8() {
	p.prepareStmt.Grow(350)
	p.prepareStmt.WriteString("SELECT time_bucket(k_timestamp, '3600s') as k_timestamp, max(usage_user), " +
		"max(usage_system), max(usage_idle), max(usage_nice), max(usage_iowait), max(usage_irq), " +
		"max(usage_softirq), max(usage_steal), max(usage_guest), max(usage_guest_nice) FROM benchmark.cpu " +
		"WHERE hostname IN ($1,$2,$3,$4,$5,$6,$7,$8) AND k_timestamp >= $9 AND k_timestamp < $10 " +
		"GROUP BY time_bucket(k_timestamp, '3600s') ORDER BY time_bucket(k_timestamp, '3600s')")
	p.formatBuf = make([]int16, 10)
	for i := 0; i < 10; i++ {
		p.formatBuf[i] = 1
	}
}

// double-groupby-1 double-groupby-5 double-groupby-all
func (p *processor) InitDoubleGroupby() {
	p.prepareStmt.Grow(350)
	p.prepareStmt.WriteString("SELECT time_bucket(k_timestamp, '3600s') as k_timestamp, hostname, avg(usage_user) " +
		"FROM benchmark.cpu WHERE k_timestamp >= $1 AND k_timestamp < $2 GROUP BY hostname, time_bucket(k_timestamp, '3600s') " +
		"ORDER BY hostname, time_bucket(k_timestamp, '3600s')")
	p.formatBuf = make([]int16, 2)
	for i := 0; i < 2; i++ {
		p.formatBuf[i] = 1
	}
}

// groupby-orderby-limit
func (p *processor) InitGroupbyOrder() {
	p.prepareStmt.Grow(350)
	p.prepareStmt.WriteString("SELECT time_bucket(k_timestamp, '60s') as k_timestamp, max(usage_user) FROM benchmark.cpu " +
		"WHERE k_timestamp < $1 GROUP BY time_bucket(k_timestamp, '60s') ORDER BY time_bucket(k_timestamp, '60s') LIMIT 5")
	p.formatBuf = make([]int16, 1)
	for i := 0; i < 1; i++ {
		p.formatBuf[i] = 1
	}
}

// high-cpu-1
func (p *processor) InitHighCpu1() {
	p.prepareStmt.Grow(350)
	p.prepareStmt.WriteString("SELECT k_timestamp,usage_user,usage_system,usage_idle,usage_nice,usage_iowait," +
		"usage_irq,usage_softirq,usage_steal,usage_guest,usage_guest_nice FROM benchmark.cpu " +
		"WHERE hostname=$1 AND usage_user > 90.0 AND k_timestamp >= $2 AND k_timestamp < $3")
	p.formatBuf = make([]int16, 3)
	for i := 0; i < 3; i++ {
		p.formatBuf[i] = 1
	}
}

// high-cpu-all
func (p *processor) InitHighCpuall() {
	p.prepareStmt.Grow(350)
	p.prepareStmt.WriteString("SELECT k_timestamp,usage_user,usage_system,usage_idle,usage_nice,usage_iowait," +
		"usage_irq,usage_softirq,usage_steal,usage_guest,usage_guest_nice FROM benchmark.cpu " +
		"WHERE usage_user > 90.0 AND k_timestamp >= $1 AND k_timestamp < $2")
	p.formatBuf = make([]int16, 2)
	for i := 0; i < 2; i++ {
		p.formatBuf[i] = 1
	}
}

// lastpoint
func (p *processor) InitLastPoint() {
	p.prepareStmt.Grow(350)
	p.prepareStmt.WriteString("SELECT last_row(k_timestamp), last_row(usage_user), last_row(usage_system), " +
		"last_row(usage_idle), last_row(usage_nice), last_row(usage_iowait), last_row(usage_irq), " +
		"last_row(usage_softirq), last_row(usage_steal), last_row(usage_guest), last_row(usage_guest_nice), " +
		"hostname FROM benchmark.cpu GROUP BY hostname")
}

// single-groupby-5-1-12
func (p *processor) InitSingleGroupby_Host1() {
	p.prepareStmt.Grow(350)
	p.prepareStmt.WriteString(`SELECT time_bucket(k_timestamp, '60s') as k_timestamp, 
    max(usage_user), max(usage_system), max(usage_idle), max(usage_nice), 
    max(usage_iowait) FROM benchmark.cpu 
    WHERE hostname=$1 AND k_timestamp >= $2 AND 
    k_timestamp < $3 GROUP BY time_bucket(k_timestamp, '60s')
    ORDER BY time_bucket(k_timestamp, '60s')`)
	p.formatBuf = make([]int16, 3)
	for i := 0; i < 3; i++ {
		p.formatBuf[i] = 1
	}
}

func (p *processor) InitSingleGroupby_Hosts() {
	p.prepareStmt.Grow(350)
	p.prepareStmt.WriteString("SELECT time_bucket(k_timestamp, '60s') as k_timestamp, max(usage_user), " +
		"max(usage_system), max(usage_idle), max(usage_nice), max(usage_iowait) FROM benchmark.cpu WHERE " +
		"hostname IN ($1,$2,$3,$4,$5,$6,$7,$8) AND k_timestamp >= $9 AND k_timestamp < $10 " +
		"GROUP BY time_bucket(k_timestamp,'60s') ORDER BY time_bucket(k_timestamp,'60s')")
	p.formatBuf = make([]int16, 10)
	for i := 0; i < 10; i++ {
		p.formatBuf[i] = 1
	}
}

func (p *processor) RunCpu1(querys []string, tableBuffer *fixedArgList) {
	// hostname
	v := strings.TrimSpace(querys[0])
	tableBuffer.Append([]byte(v))
	//time
	settime(querys[1], querys[2], tableBuffer)
}

func (p *processor) RunCpu8(querys []string, tableBuffer *fixedArgList) {
	// 8hostnames
	for i := 0; i < 8; i++ {
		v := strings.TrimSpace(querys[i])
		tableBuffer.Append([]byte(v))
	}
	//time
	settime(querys[8], querys[9], tableBuffer)
}

func (p *processor) RunDoubleGroup(querys []string, tableBuffer *fixedArgList) {
	settime(querys[0], querys[1], tableBuffer)
}

func (p *processor) RunGroupbyOrder(querys []string, tableBuffer *fixedArgList) {
	num1, _ := strconv.ParseInt(querys[0], 10, 64)
	tableBuffer.Emplace(uint64(num1*1000) - microsecFromUnixEpochToY2K)
}

func (p *processor) RunHighCpu1(querys []string, tableBuffer *fixedArgList) {
	// hostname
	v := strings.TrimSpace(querys[0])
	tableBuffer.Append([]byte(v))
	//time
	settime(querys[1], querys[2], tableBuffer)
}

func (p *processor) RunHighCpu8(querys []string, tableBuffer *fixedArgList) {
	// hostname
	v := strings.TrimSpace(querys[0])
	tableBuffer.Append([]byte(v))
	//time
	settime(querys[1], querys[2], tableBuffer)
}

func (p *processor) RunHighCpuall(querys []string, tableBuffer *fixedArgList) {
	//time
	settime(querys[0], querys[1], tableBuffer)
}

func (p *processor) RunLastPoint(querys []string, tableBuffer *fixedArgList) {
	////time
	//settime(querys[0], querys[1], tableBuffer)
}

func (p *processor) Runsinglegroupby_Host1(querys []string, tableBuffer *fixedArgList) {
	// hostname
	v := strings.TrimSpace(querys[0])
	tableBuffer.Append([]byte(v))
	//time
	settime(querys[1], querys[2], tableBuffer)
}

func (p *processor) Runsinglegroupby_Hosts(querys []string, tableBuffer *fixedArgList) {
	// 8hostnames
	for i := 0; i < 8; i++ {
		v := strings.TrimSpace(querys[i])
		tableBuffer.Append([]byte(v))
	}
	settime(querys[8], querys[9], tableBuffer)
}

func settime(starttime string, endtime string, tableBuffer *fixedArgList) {
	//time
	num1, _ := strconv.ParseInt(starttime, 10, 64)
	tableBuffer.Emplace(uint64(num1*1000) - microsecFromUnixEpochToY2K)
	//time
	num2, _ := strconv.ParseInt(endtime, 10, 64)
	tableBuffer.Emplace(uint64(num2*1000) - microsecFromUnixEpochToY2K)
}

func (p *processor) RunSelect(querytype string, querys []string, tableBuffer *fixedArgList) {
	switch querytype {
	// cpu-only
	case QueryTypeCPUMaxAll1:
		p.RunCpu1(querys, tableBuffer)
	case QueryTypeCPUMaxAll8:
		p.RunCpu8(querys, tableBuffer)
	case QueryTypeDoubleGroupby1, QueryTypeDoubleGroupby5, QueryTypeDoubleGroupbyAll:
		p.RunDoubleGroup(querys, tableBuffer)
	case QueryTypeGroupbyOrderbyLimit:
		p.RunGroupbyOrder(querys, tableBuffer)
	case QueryTypeHighCPU1:
		p.RunHighCpu1(querys, tableBuffer)
	case QueryTypeHighCPUAll:
		p.RunHighCpuall(querys, tableBuffer)
	case QueryTypeLastPoint:
		p.RunLastPoint(querys, tableBuffer)
	case QueryTypeSingleGroupby1_1_1, QueryTypeSingleGroupby1_1_12,
		QueryTypeSingleGroupby5_1_1, QueryTypeSingleGroupby5_1_12:
		p.Runsinglegroupby_Host1(querys, tableBuffer)
	case QueryTypeSingleGroupby1_8_1, QueryTypeSingleGroupby5_8_1:
		p.Runsinglegroupby_Hosts(querys, tableBuffer)
	// iot
	case QueryTypeLastLoc, QueryTypeSingleLastLoc, QueryTypeLowFuel, QueryTypeHighLoad:
		p.RunLastLoc(querys, tableBuffer)
	case QueryTypeStationaryTrucks:
		p.RunStationaryTrucks(querys, tableBuffer)
	case QueryTypeLongDrivingSessions:
		p.RunLongDrivingSessions(querys, tableBuffer)
	case QueryTypeLongDailySessions:
		p.RunLongDailySessions(querys, tableBuffer)
	case QueryTypeAvgVsProjFuelConsumption:
		p.Run(querys, tableBuffer)
	case QueryTypeAvgDailyDrivingDuration, QueryTypeAvgDailyDrivingSession,
		QueryTypeDailyActivity, QueryTypeBreakdownFrequency:
		p.RunDuration(querys, tableBuffer)
	case QueryTypeAvgLoad:
		p.Run(querys, tableBuffer)
	}
}
