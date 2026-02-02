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
	sql := fmt.Sprintf("create ts database %s partition interval 1d;", dbName)
	_, err := d.db.Connection.Exec(ctx, sql)
	if err != nil && !strings.Contains(err.Error(), "already exists") {
		panic(fmt.Sprintf("kwdb create database failed,err :%s", err))
	}

	if d.opts.Case == "cpu-only" {
		sql := fmt.Sprintf("create table %s.cpu (k_timestamp timestamp not null,usage_user bigint not null,usage_system bigint not null,usage_idle bigint not null,usage_nice bigint not null,"+
			"usage_iowait bigint not null,usage_irq bigint not null,usage_softirq bigint not null,usage_steal bigint not null,usage_guest bigint not null,usage_guest_nice bigint not null) "+
			"tags (hostname char(30) not null,region char(30),datacenter char(30),rack char(30),os char(30),arch char(30),team char(30),service char(30),"+
			"service_version char(30),service_environment char(30)) primary tags(hostname)", dbName)
		_, err = d.db.Connection.Exec(ctx, sql)
		if err != nil && !strings.Contains(err.Error(), "already exists") {
			panic(fmt.Sprintf("kwdb create table failed,err :%s", err))
		}

		if d.opts.Partition {
			sqlpartition := fmt.Sprintf("alter table %s.cpu partition by hashpoint(partition p0 values from (0) to (666), partition p1 values from (666) to (1332), partition p2 values from (1332) to (2000));", dbName)
			_, err = d.db.Connection.Exec(ctx, sqlpartition)

			sqlpartition = fmt.Sprintf("ALTER PARTITION p0 OF TABLE %s.cpu CONFIGURE ZONE USING lease_preferences = '[[+region=NODE1]]',constraints = '{\"+region=NODE1\":1}',num_replicas=3;"+
				"ALTER PARTITION p1 OF TABLE %s.cpu CONFIGURE ZONE USING lease_preferences = '[[+region=NODE2]]',constraints = '{\"+region=NODE2\":1}',num_replicas=3;"+
				"ALTER PARTITION p2 OF TABLE %s.cpu CONFIGURE ZONE USING lease_preferences = '[[+region=NODE3]]',constraints = '{\"+region=NODE3\":1}',num_replicas=3;", dbName, dbName, dbName)
			_, err = d.db.Connection.Exec(ctx, sqlpartition)
			if err != nil {
				panic(fmt.Sprintf("kwdb alter partition failed,err :%s", err))
			}
			// 暂停一分钟
			time.Sleep(1 * time.Minute)
		}
	} else if d.opts.Case == "iot" {
		fmt.Println("create iot tables")
		readings := fmt.Sprintf("create table %s.readings (k_timestamp timestamp NOT NULL,latitude FLOAT8 NOT NULL,longitude FLOAT8 NOT NULL,elevation FLOAT8 NOT NULL,velocity FLOAT8 NOT NULL,heading FLOAT8 NOT NULL,grade FLOAT8 NOT NULL,fuel_consumption FLOAT8 NOT NULL) tags (name VARCHAR(30) NOT NULL,fleet VARCHAR(30),driver VARCHAR(30),model VARCHAR(30),device_version VARCHAR(30),load_capacity FLOAT8,fuel_capacity FLOAT8,nominal_fuel_consumption FLOAT8) primary tags(name)", dbName)
		_, err := d.db.Connection.Exec(ctx, readings)
		if err != nil && !strings.Contains(err.Error(), "already exists") {
			panic(fmt.Sprintf("kwdb create table readings failed,err :%s", err))
		}

		diagnostics := fmt.Sprintf("create table %s.diagnostics (k_timestamp timestamp NOT NULL,fuel_state FLOAT8 NOT NULL,current_load FLOAT8 NOT NULL,status INT8 NOT NULL) tags (name VARCHAR(30) NOT NULL,fleet VARCHAR(30),driver VARCHAR(30),model VARCHAR(30),device_version VARCHAR(30),load_capacity FLOAT8,fuel_capacity FLOAT8,nominal_fuel_consumption FLOAT8) primary tags(name)", dbName)
		_, err = d.db.Connection.Exec(ctx, diagnostics)
		if err != nil && !strings.Contains(err.Error(), "already exists") {
			panic(fmt.Sprintf("kwdb create table diagnostics failed,err :%s", err))
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
