package kwdb

import "C"
import (
	"context"
	"fmt"
	"github.com/timescale/tsbs/pkg/targets"
	"github.com/timescale/tsbs/pkg/targets/kwdb/commonpool"
	"log"
	"strings"
	"time"
)

var fatal = log.Fatalf

type dbCreator struct {
	opts *LoadingOptions
	ds   targets.DataSource
	db   *commonpool.Conn
}

var IOTPRE = []string{"readings", "diagnostics"}
var DEVOPSPRE = []string{"cpu", "diskio", "disk", "kernel", "mem", "net", "nginx", "postgresl", "redis"}

func (d *dbCreator) Init() {
	db, err := commonpool.GetConnection(d.opts.User, d.opts.Pass, d.opts.Host, d.opts.CertDir, d.opts.Port)
	if err != nil {
		panic(fmt.Sprintf("kwdb can not get connection %s", err.Error()))
	}
	d.db = db
}

func (d *dbCreator) DBExists(dbName string) bool {
	return true
}

func (d *dbCreator) CreateDB(dbName string) error {
	ctx := context.Background()
	// 创建时序数据库
	sql := fmt.Sprintf("create ts database %s ", dbName)
	_, err := d.db.Connection.Exec(ctx, sql)
	if err != nil && !strings.Contains(err.Error(), "already exists") {
		panic(fmt.Sprintf("kwdb create database failed,err :%s", err))
	}

	if d.opts.Case == "cpu-only" {
		sql := fmt.Sprintf("create table %s.cpu (k_timestamp timestamp not null,usage_user bigint not null,usage_system bigint not null,usage_idle bigint not null,usage_nice bigint not null,usage_iowait bigint not null,usage_irq bigint not null,usage_softirq bigint not null,usage_steal bigint not null,usage_guest bigint not null,usage_guest_nice bigint not null) tags (hostname char(30) not null,region char(30),datacenter char(30),rack char(30),os char(30),arch char(30),team char(30),service char(30),service_version char(30),service_environment char(30)) primary tags(hostname)", dbName)
		_, err = d.db.Connection.Exec(ctx, sql)

		if d.opts.Partition {
			sqlpartition := fmt.Sprintf("alter table %s.cpu partition by hashpoint(partition p0 values from (0) to (666), partition p1 values from (666) to (1332), partition p2 values from (1332) to (2000));", dbName)
			_, err = d.db.Connection.Exec(ctx, sqlpartition)

			sqlpartition = fmt.Sprintf("ALTER PARTITION p0 OF TABLE benchmark.cpu CONFIGURE ZONE USING lease_preferences = '[[+region=NODE1]]',constraints = '{\"+region=NODE1\":1}',num_replicas=3;" +
				"ALTER PARTITION p1 OF TABLE benchmark.cpu CONFIGURE ZONE USING lease_preferences = '[[+region=NODE2]]',constraints = '{\"+region=NODE2\":1}',num_replicas=3;" +
				"ALTER PARTITION p2 OF TABLE benchmark.cpu CONFIGURE ZONE USING lease_preferences = '[[+region=NODE3]]',constraints = '{\"+region=NODE3\":1}',num_replicas=3;")
			_, err = d.db.Connection.Exec(ctx, sqlpartition)

			// 暂停一分钟
			time.Sleep(1 * time.Minute)
		}
		if err != nil && !strings.Contains(err.Error(), "already exists") {
			panic(fmt.Sprintf("kwdb create table failed,err :%s", err))
		}

	} else {
		panic(fmt.Sprintf("kwdb cannot support this use-case '%s', currently only supports cpu-only in devops", d.opts.Case))
	}
	return nil
}

func (d *dbCreator) RemoveOldDB(dbName string) error {
	//str := strings.Split(dbName, "_")
	ctx := context.Background()

	sql := fmt.Sprintf("drop database %s", dbName)
	_, err := d.db.Connection.Exec(ctx, sql)
	if err != nil && !strings.Contains(err.Error(), "does not exist") {
		panic(fmt.Sprintf("kwdb drop database failed,err :%s", err))
	}
	return nil
}

func (d *dbCreator) Close() {
	if d.db != nil {
		d.db.Put()
	}
}
