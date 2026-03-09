package kwdb

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/timescale/tsbs/pkg/targets"
	"github.com/timescale/tsbs/pkg/targets/kwdb/commonpool"
)

type syncCSI struct {
	m sync.Map //table:ctx
}

const (
	Size1M            = 1 * 1024 * 1024
	LenReadings       = 115
	LenDiagnostics    = 80
	cpuCreatePrefix   = "insert into %s.cpu (hostname,region,datacenter,rack,os,arch,team,service,service_version,service_environment) values"
	cpuInsertPrefix   = "insert into %s.cpu (k_timestamp,usage_user,usage_system,usage_idle,usage_nice,usage_iowait,usage_irq,usage_softirq,usage_steal,usage_guest,usage_guest_nice,hostname) values"
	readingsSuffix    = "readings"
	readingsPrefix    = "insert into %s.readings (k_timestamp,latitude,longitude,elevation,velocity,heading,grade,fuel_consumption,name)values"
	diagnosticsPrefix = "insert into %s.diagnostics (k_timestamp,fuel_state,current_load,status,name)values"
)

type Ctx struct {
	c      context.Context
	cancel context.CancelFunc
}

var globalSCI = &syncCSI{}

const (
	cpuSQLKnown = iota + 1
	cpuSQLUnknown
)

type processorInsert struct {
	opts   *LoadingOptions
	dbName string
	sci    *syncCSI
	_db    *commonpool.Conn
	wg     *sync.WaitGroup
	buf    *bytes.Buffer
}

func newProcessorInsert(opts *LoadingOptions, dbName string) *processorInsert {
	return &processorInsert{opts: opts, dbName: dbName, sci: globalSCI, wg: &sync.WaitGroup{}, buf: &bytes.Buffer{}}
}

func (p *processorInsert) Init(proNum int, doLoad, _ bool) {
	if !doLoad {
		return
	}
	p.buf.Grow(Size1M)
	var err error
	p._db, err = commonpool.GetConnection(p.opts.User, p.opts.Pass, p.opts.Host, p.opts.CertDir, p.opts.Port)

	if err != nil {
		panic(err)
	}

}

func buildCPUCreateSQL(dbName string, createRows []*point) string {
	if len(createRows) == 0 {
		return ""
	}

	prefix := fmt.Sprintf(cpuCreatePrefix, dbName)
	totalLen := len(prefix)
	for _, row := range createRows {
		totalLen += len(row.sql) + 1
	}

	var sqlBuilder strings.Builder
	sqlBuilder.Grow(totalLen)
	sqlBuilder.WriteString(prefix)

	for i, row := range createRows {
		if i > 0 {
			sqlBuilder.WriteByte(',')
		}
		sqlBuilder.WriteString(row.sql)
	}

	return sqlBuilder.String()
}

func buildCPUInsertSQL(dbName string, batches map[string][]string, route func(hostname string) int) (sql1, sql2 string, rowCnt uint64) {
	if len(batches) == 0 {
		return "", "", 0
	}

	prefix := fmt.Sprintf(cpuInsertPrefix, dbName)
	var sqlBuilder1, sqlBuilder2 strings.Builder
	sqlBuilder1.Grow(len(prefix))
	sqlBuilder2.Grow(len(prefix))
	sqlBuilder1.WriteString(prefix)
	sqlBuilder2.WriteString(prefix)
	hasSQL1, hasSQL2 := false, false

	for hostname, sqls := range batches {
		rowCnt += uint64(len(sqls))
		if route(hostname) == cpuSQLKnown {
			hasSQL1 = appendCPUSQLRows(&sqlBuilder1, sqls, hasSQL1)
			continue
		}
		hasSQL2 = appendCPUSQLRows(&sqlBuilder2, sqls, hasSQL2)
	}

	if hasSQL1 {
		sql1 = sqlBuilder1.String()
	}
	if hasSQL2 {
		sql2 = sqlBuilder2.String()
	}

	return sql1, sql2, rowCnt
}

func appendCPUSQLRows(builder *strings.Builder, sqls []string, hasRows bool) bool {
	for _, sql := range sqls {
		if hasRows {
			builder.WriteByte(',')
		}
		builder.WriteString(sql)
		hasRows = true
	}
	return hasRows
}

func (p *processorInsert) ProcessBatch(b targets.Batch, doLoad bool) (metricCount, rowCount uint64) {
	batches := b.(*hypertableArr)
	rowCnt := uint64(0)
	metricCnt := batches.totalMetric
	if !doLoad {
		for _, sqls := range batches.m {
			rowCnt += uint64(len(sqls))
		}
		return metricCnt, rowCnt
	}
	p.buf.Reset()
	var deviceNum int
	if p.opts.Case == "cpu-only" {
		if p.opts.DoCreate && len(batches.createSql) > 0 {
			deviceContexts := make(map[string]*Ctx, len(batches.createSql))
			for _, row := range batches.createSql {
				c, cancel := context.WithCancel(context.Background())
				ctx := &Ctx{
					c:      c,
					cancel: cancel,
				}
				actual, _ := p.sci.m.LoadOrStore(row.device, ctx)
				deviceContexts[row.device] = actual.(*Ctx)
			}

			sql := buildCPUCreateSQL(p.dbName, batches.createSql)
			_, err := p._db.Connection.Exec(context.Background(), sql)
			if err != nil {
				panic(fmt.Sprintf("kwdb insert data failed,err :%s", err))
			}

			for _, ctx := range deviceContexts {
				ctx.cancel()
			}
		}

		sql1, sql2, batchRows := buildCPUInsertSQL(p.dbName, batches.m, func(hostname string) int {
			v, ok := p.sci.m.Load(hostname)
			if ok {
				<-v.(*Ctx).c.Done()
				return cpuSQLKnown
			}

			// wait for allTag data inserted
			allTagC, allTagCancel := context.WithCancel(context.Background())
			allTagCtx := &Ctx{
				c:      allTagC,
				cancel: allTagCancel,
			}
			allTagActual, _ := p.sci.m.LoadOrStore(hostname, allTagCtx)
			<-allTagActual.(*Ctx).c.Done()
			return cpuSQLUnknown
		})
		rowCnt += batchRows

		if sql1 != "" {
			_, err := p._db.Connection.Exec(context.Background(), sql1)
			if err != nil {
				panic(fmt.Sprintf("kwdb insert data failed!,err :%s", err))
			}
		}
		if sql2 != "" {
			_, err := p._db.Connection.Exec(context.Background(), sql2)
			if err != nil {
				panic(fmt.Sprintf("kwdb insert data failed!,err :%s", err))
			}
		}

		batches.Reset()
	} else if p.opts.Case == "iot" {
		var br, bd strings.Builder
		if p.opts.DoCreate {
			br.WriteString(fmt.Sprintf("insert into %s.readings(name,fleet,driver,model,device_version,load_capacity,fuel_capacity,nominal_fuel_consumption)values", p.dbName))
			bd.WriteString(fmt.Sprintf("insert into %s.diagnostics(name,fleet,driver,model,device_version,load_capacity,fuel_capacity,nominal_fuel_consumption)values", p.dbName))
			lenbr, lenbd := br.Len(), bd.Len()
			batcheslen := len(batches.createSql)
			for i, row := range batches.createSql {
				c, cancel := context.WithCancel(context.Background())
				ctx := &Ctx{
					c:      c,
					cancel: cancel,
				}
				actual, _ := p.sci.m.LoadOrStore(row.device, ctx)
				if row.template == "readings" {
					br.WriteString(row.sql)
					if i < batcheslen-1 {
						br.WriteString(",")
					}
				} else {
					bd.WriteString(row.sql)
					if i < batcheslen-1 {
						bd.WriteString(",")
					}
				}
				actual.(*Ctx).cancel()
				continue
			}
			if batches.createSql != nil {
				if br.Len() > lenbr {
					_, err := p._db.Connection.Exec(context.Background(), br.String())
					if err != nil {
						panic(fmt.Sprintf("kwdb insert readings data failed,err :%s", err))
					}
				}
				if bd.Len() > lenbd {
					_, err := p._db.Connection.Exec(context.Background(), bd.String())
					if err != nil {
						panic(fmt.Sprintf("kwdb insert diagnostics data failed,err :%s", err))
					}
				}

			}
		}

		p.buf.Reset()
		var b1, b2, b3, b4 strings.Builder
		fmt.Fprintf(&b1, readingsPrefix, p.dbName)
		fmt.Fprintf(&b2, diagnosticsPrefix, p.dbName)
		fmt.Fprintf(&b3, readingsPrefix, p.dbName)
		fmt.Fprintf(&b4, diagnosticsPrefix, p.dbName)
		cnt1, cnt2 := 0, 0
		for hostname, sqls := range batches.m {
			rowCnt += uint64(len(sqls))
			csvSQL := strings.Join(sqls, ",")
			v, ok := p.sci.m.Load(hostname)
			if ok {
				<-v.(*Ctx).c.Done()
				if strings.HasPrefix(hostname, readingsSuffix) {
					b1.WriteString(csvSQL)
				} else { //means diagnostics
					b2.WriteString(csvSQL)
				}
				cnt1 += len(sqls)
			} else {
				// wait for allTag data inserted
				allTagC, allTagCancel := context.WithCancel(context.Background())
				allTagCtx := &Ctx{
					c:      allTagC,
					cancel: allTagCancel,
				}
				allTagActual, _ := p.sci.m.LoadOrStore(hostname, allTagCtx)
				<-allTagActual.(*Ctx).c.Done()

				if strings.HasPrefix(hostname, readingsSuffix) {
					b3.WriteString(csvSQL)
				} else { //means diagnostics
					b4.WriteString(csvSQL)
				}
				cnt2++
			}
		}
		if cnt1+cnt2 == int(batches.cnt) {
			execSQL := func(sqlStr strings.Builder, expectedLen int, sqlType string) {
				if sqlStr.Len() != expectedLen {
					_, err := p._db.Connection.Exec(context.Background(), sqlStr.String())
					if err != nil {
						fmt.Println(expectedLen, sqlStr.Len())
						panic(fmt.Sprintf("kwdb insert %s data failed! err: %s", sqlType, err))
					}
				}
			}

			execSQL(b1, LenReadings+len(p.dbName), "readings1")
			execSQL(b2, LenDiagnostics+len(p.dbName), "diagnostics1")

			if cnt2 != 0 {
				execSQL(b3, LenReadings+len(p.dbName), "readings2")
				execSQL(b4, LenDiagnostics+len(p.dbName), "diagnostics2")
			}
		}

		batches.Reset()
	}

	return metricCnt + uint64(deviceNum)*20, rowCnt + uint64(deviceNum)

}

func (p *processorInsert) Close(doLoad bool) {
	if doLoad {
		p._db.Put()
	}
}
