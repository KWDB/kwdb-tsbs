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
	readingsSuffix    = "readings"
	readingsPrefix    = "insert into %s.readings (k_timestamp,latitude,longitude,elevation,velocity,heading,grade,fuel_consumption,name)values"
	diagnosticsPrefix = "insert into %s.diagnostics (k_timestamp,fuel_state,current_load,status,name)values"
)

type Ctx struct {
	c      context.Context
	cancel context.CancelFunc
}

var globalSCI = &syncCSI{}

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
			deviceContexts := make(map[string]*Ctx)
			for _, row := range batches.createSql {
				c, cancel := context.WithCancel(context.Background())
				ctx := &Ctx{
					c:      c,
					cancel: cancel,
				}
				actual, _ := p.sci.m.LoadOrStore(row.device, ctx)
				deviceContexts[row.device] = actual.(*Ctx)
			}

			var sqlBuilder strings.Builder
			sqlBuilder.WriteString(fmt.Sprintf("insert into %s.cpu (hostname,region,datacenter,rack,os,arch,team,service,service_version,service_environment) values", p.dbName))

			for i, row := range batches.createSql {
				if i > 0 {
					sqlBuilder.WriteString(",")
				}
				sqlBuilder.WriteString(row.sql)
			}

			sql := sqlBuilder.String()
			_, err := p._db.Connection.Exec(context.Background(), sql)
			if err != nil {
				panic(fmt.Sprintf("kwdb insert data failed,err :%s", err))
			}

			for _, ctx := range deviceContexts {
				ctx.cancel()
			}
		}

		p.buf.Reset()
		tagsname := fmt.Sprintf("usage_user,usage_system,usage_idle,usage_nice,usage_iowait,usage_irq,usage_softirq,usage_steal,usage_guest,usage_guest_nice")
		sql1 := fmt.Sprintf("insert into %s.cpu (k_timestamp,%s,hostname) values", p.dbName, tagsname)
		sql2 := sql1
		cnt1, cnt2 := 0, 0
		for hostname, sqls := range batches.m {
			rowCnt += uint64(len(sqls))
			// var csvSQL string
			csvSQL := strings.Join(sqls, ",")
			v, ok := p.sci.m.Load(hostname)
			// fmt.Println(hostname)
			if ok {
				<-v.(*Ctx).c.Done()
				sql1 += csvSQL + ","
				cnt1++
			} else {
				// wait for allTag data inserted
				allTagC, allTagCancel := context.WithCancel(context.Background())
				allTagCtx := &Ctx{
					c:      allTagC,
					cancel: allTagCancel,
				}
				allTagActual, _ := p.sci.m.LoadOrStore(hostname, allTagCtx)
				<-allTagActual.(*Ctx).c.Done()

				sql2 += csvSQL + ","
				cnt2++
			}
		}
		if cnt1+cnt2 == len(batches.m) {
			if cnt1 != 0 {
				sql1 = sql1[:len(sql1)-1]
				_, err := p._db.Connection.Exec(context.Background(), sql1)
				if err != nil {
					panic(fmt.Sprintf("kwdb insert data failed!,err :%s", err))
				}
			}
			if cnt2 != 0 {
				sql2 = sql2[:len(sql2)-1]
				_, err2 := p._db.Connection.Exec(context.Background(), sql2)
				if err2 != nil {
					panic(fmt.Sprintf("kwdb insert data failed!,err :%s", err2))
				}
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
