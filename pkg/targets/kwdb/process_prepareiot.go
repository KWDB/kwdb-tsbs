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

// fastParseInt attempts to parse an integer string with minimal overhead.
// Returns the parsed value and true if successful, or 0 and false if parsing failed.
// This function handles simple decimal integers (with optional leading minus sign)
// and is designed for the common case of numeric literals.
func fastParseInt(s string) (int64, bool) {
	if len(s) == 0 {
		return 0, false
	}

	var neg bool
	var i int

	// Handle sign
	if s[0] == '-' {
		neg = true
		i = 1
		if len(s) == 1 {
			return 0, false
		}
	} else if s[0] == '+' {
		i = 1
		if len(s) == 1 {
			return 0, false
		}
	}

	// Parse digits
	var n uint64
	for ; i < len(s); i++ {
		ch := s[i]
		if ch < '0' || ch > '9' {
			return 0, false
		}
		// Check for overflow before multiplication
		if n > (math.MaxUint64-9)/10 {
			return 0, false
		}
		n = n*10 + uint64(ch-'0')
	}

	// Check for overflow based on sign
	if neg {
		if n > uint64(-math.MinInt64) {
			return 0, false
		}
		return -int64(n), true
	}
	if n > math.MaxInt64 {
		return 0, false
	}
	return int64(n), true
}

var float64pow10 = [...]float64{
	1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9,
	1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16,
}

var (
	inf = math.Inf(1)
	nan = math.NaN()
)

func ParseFloatFast(s string) (float64, error) {
	if len(s) == 0 {
		return 0, fmt.Errorf("cannot parse float64 from empty string")
	}

	i := uint(0)
	minus := s[0] == '-'
	if minus {
		i++
		if i >= uint(len(s)) {
			return 0, fmt.Errorf("cannot parse float64 from %q", s)
		}
	}

	if s[i] == '.' && (i+1 >= uint(len(s)) || s[i+1] < '0' || s[i+1] > '9') {
		return 0, fmt.Errorf("missing integer and fractional part in %q", s)
	}

	d := uint64(0)
	j := i

	for i < uint(len(s)) {
		if s[i] >= '0' && s[i] <= '9' {
			d = d*10 + uint64(s[i]-'0')
			i++
			if i > 18 {
				f, err := strconv.ParseFloat(s, 64)
				if err != nil && !math.IsInf(f, 0) {
					return 0, err
				}
				return f, nil
			}
			continue
		}
		break
	}

	// inf, infinity, nan
	if i <= j && s[i] != '.' {
		ss := s[i:]
		if strings.HasPrefix(ss, "+") {
			ss = ss[1:]
		}
		if strings.EqualFold(ss, "inf") || strings.EqualFold(ss, "infinity") {
			if minus {
				return -inf, nil
			}
			return inf, nil
		}
		if strings.EqualFold(ss, "nan") {
			return nan, nil
		}
		return 0, fmt.Errorf("unparsed tail left after parsing float64 from %q: %q", s, ss)
	}

	f := float64(d)

	if i >= uint(len(s)) {
		if minus {
			f = -f
		}
		return f, nil
	}

	if s[i] == '.' {
		i++
		if i >= uint(len(s)) {
			if minus {
				f = -f
			}
			return f, nil
		}

		k := i
		for i < uint(len(s)) {
			if s[i] >= '0' && s[i] <= '9' {
				d = d*10 + uint64(s[i]-'0')
				i++
				if i-j >= uint(len(float64pow10)) {
					f, err := strconv.ParseFloat(s, 64)
					if err != nil && !math.IsInf(f, 0) {
						return 0, fmt.Errorf("cannot parse mantissa in %q: %s", s, err)
					}
					return f, nil
				}
				continue
			}
			break
		}

		if i < k {
			return 0, fmt.Errorf("cannot find mantissa in %q", s)
		}

		f = float64(d) / float64pow10[i-k]

		if i >= uint(len(s)) {
			if minus {
				f = -f
			}
			return f, nil
		}
	}

	if s[i] == 'e' || s[i] == 'E' {
		i++
		if i >= uint(len(s)) {
			return 0, fmt.Errorf("cannot parse exponent in %q", s)
		}

		expMinus := false
		if s[i] == '+' || s[i] == '-' {
			expMinus = s[i] == '-'
			i++
			if i >= uint(len(s)) {
				return 0, fmt.Errorf("cannot parse exponent in %q", s)
			}
		}

		exp := int16(0)
		j := i
		for i < uint(len(s)) {
			if s[i] >= '0' && s[i] <= '9' {
				exp = exp*10 + int16(s[i]-'0')
				i++
				if exp > 300 {
					f, err := strconv.ParseFloat(s, 64)
					if err != nil && !math.IsInf(f, 0) {
						return 0, fmt.Errorf("cannot parse exponent in %q: %s", s, err)
					}
					return f, nil
				}
				continue
			}
			break
		}

		if i <= j {
			return 0, fmt.Errorf("cannot parse exponent in %q", s)
		}

		if expMinus {
			exp = -exp
		}
		f *= math.Pow10(int(exp))

		if i >= uint(len(s)) {
			if minus {
				f = -f
			}
			return f, nil
		}
	}

	return 0, fmt.Errorf("cannot parse float64 from %q", s)
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
			sLen := len(s)

			if isReadings {
				p.parseReadingsRow(s, sLen, tableBuffer)
			} else {
				p.parseDiagnosticsRow(s, sLen, tableBuffer)
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

	return metricCnt, rowCnt
}

// parseReadingsRow
func (p *prepareProcessoriot) parseReadingsRow(s string, sLen int, tableBuffer *fixedArgList) {
	start := 0
	fieldIdx := 0

	for pos := 0; pos <= sLen; pos++ {
		if pos == sLen || s[pos] == ',' {
			v := s[start:pos]

			switch fieldIdx {
			case 0:
				num, ok := fastParseInt(v)
				if !ok {
					num, _ = strconv.ParseInt(v, 10, 64)
				}
				tableBuffer.Emplace(uint64(num*1000) - microsecFromUnixEpochToY2K + 8*3600*1000000)
			case 8:
				if q1 := strings.IndexByte(v, '\''); q1 >= 0 {
					if q2 := strings.IndexByte(v[q1+1:], '\''); q2 >= 0 {
						tableBuffer.Append([]byte(v[q1+1 : q1+1+q2]))
					}
				}
			default:
				num, err := ParseFloatFast(v)
				if err != nil {
					num, _ = strconv.ParseFloat(v, 64)
				}
				tableBuffer.EmplaceFloat64(num)
			}

			start = pos + 1
			fieldIdx++
		}
	}
}

// parseDiagnosticsRow
func (p *prepareProcessoriot) parseDiagnosticsRow(s string, sLen int, tableBuffer *fixedArgList) {
	start := 0
	fieldIdx := 0

	for pos := 0; pos <= sLen; pos++ {
		if pos == sLen || s[pos] == ',' {
			v := s[start:pos]

			switch fieldIdx {
			case 0:
				num, ok := fastParseInt(v)
				if !ok {
					num, _ = strconv.ParseInt(v, 10, 64)
				}
				tableBuffer.Emplace(uint64(num*1000) - microsecFromUnixEpochToY2K + 8*3600*1000000)
			case 3:
				num, ok := fastParseInt(v)
				if !ok {
					num, _ = strconv.ParseInt(v, 10, 64)
				}
				tableBuffer.Emplace(uint64(num))
			case 4:
				if q1 := strings.IndexByte(v, '\''); q1 >= 0 {
					if q2 := strings.IndexByte(v[q1+1:], '\''); q2 >= 0 {
						tableBuffer.Append([]byte(v[q1+1 : q1+1+q2]))
					}
				}
			default:
				num, err := ParseFloatFast(v)
				if err != nil {
					num, _ = strconv.ParseFloat(v, 64)
				}
				tableBuffer.EmplaceFloat64(num)
			}

			start = pos + 1
			fieldIdx++
		}
	}
}

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
