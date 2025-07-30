package main

import (
	"strings"
)

// 查询类型
const (
	QueryTypeLastLoc                  = "last-loc"
	QueryTypeSingleLastLoc            = "single-last-loc"
	QueryTypeLowFuel                  = "low-fuel"
	QueryTypeHighLoad                 = "high-load"
	QueryTypeStationaryTrucks         = "stationary-trucks"
	QueryTypeLongDrivingSessions      = "long-driving-sessions"
	QueryTypeLongDailySessions        = "long-daily-sessions"
	QueryTypeAvgVsProjFuelConsumption = "avg-vs-projected-fuel-consumption"
	QueryTypeAvgDailyDrivingDuration  = "avg-daily-driving-duration"
	QueryTypeAvgDailyDrivingSession   = "avg-daily-driving-session"
	QueryTypeAvgLoad                  = "avg-load"
	QueryTypeDailyActivity            = "daily-activity"
	QueryTypeBreakdownFrequency       = "breakdown-frequency"
)

func (p *processor) InitLastLoc() {
	p.prepareStmt.Grow(150)
	p.prepareStmt.WriteString("SELECT last(k_timestamp), name, last(driver), last(latitude) as latitude, last(longitude) as longitude FROM benchmark.readings WHERE fleet=$1 and name IS NOT NULL GROUP BY name")
	p.formatBuf = make([]int16, 1)
	p.formatBuf[0] = 1
}

func (p *processor) InitLowFuel() {
	p.prepareStmt.Grow(150)
	p.prepareStmt.WriteString("SELECT last(k_timestamp), last(latitude), last(longitude) FROM benchmark.readings WHERE name IN ($1) GROUP BY name")
	p.formatBuf = make([]int16, 1)
	p.formatBuf[0] = 1
}

func (p *processor) InitSingleLastLoc() {
	p.prepareStmt.Grow(150)
	p.prepareStmt.WriteString("SELECT last_row(k_timestamp) ,name, last(driver) as driver ,last(fuel_state) as fuel_state FROM benchmark.diagnostics where fleet = $1 and fuel_state <= 0.1   GROUP BY name")
	p.formatBuf = make([]int16, 1)
	p.formatBuf[0] = 1
}

func (p *processor) InitHighLoad() {
	p.prepareStmt.Grow(150)
	p.prepareStmt.WriteString("SELECT ts,name,driver,current_load,load_capacity FROM (SELECT last_row(k_timestamp) as ts,name,last(driver) as driver , last(current_load) as current_load,last(load_capacity) as  load_capacity FROM  benchmark.diagnostics WHERE fleet = $1 group by name) WHERE current_load>= (0.9 * load_capacity)")
	p.formatBuf = make([]int16, 1)
	p.formatBuf[0] = 1
}

func (p *processor) InitStationaryTrucks() {
	p.prepareStmt.Grow(150)
	p.prepareStmt.WriteString("select name,driver from (SELECT name,driver,avg(velocity) as mean_velocity from benchmark.readings  where fleet =$1 and k_timestamp > $2 AND k_timestamp <= $3 group BY name,driver) WHERE mean_velocity < 1")
	p.formatBuf = make([]int16, 3)
	for i := 0; i < 3; i++ {
		p.formatBuf[i] = 1
	}
}

func (p *processor) InitLongDrivingSessions() {
	p.prepareStmt.Grow(300)
	p.prepareStmt.WriteString("SELECT name,driver FROM (SELECT name,last(driver) as driver,avg(velocity) as mean_velocity FROM benchmark.readings WHERE fleet = $1 AND k_timestamp > $2 AND k_timestamp <= $3 group BY name, time_bucket(k_timestamp, '600s')) WHERE mean_velocity > 1 GROUP BY name,driver having count(*) > 22")
	p.formatBuf = make([]int16, 3)
	for i := 0; i < 3; i++ {
		p.formatBuf[i] = 1
	}
}

func (p *processor) InitLongDailySessions() {
	p.prepareStmt.Grow(300)
	p.prepareStmt.WriteString("SELECT name,last(driver) as driver FROM benchmark.readings WHERE fleet =$1 AND k_timestamp > $2 AND k_timestamp <= $3 group BY name having count(*) > 60 and avg(velocity) > 1 ")
	p.formatBuf = make([]int16, 3)
	for i := 0; i < 3; i++ {
		p.formatBuf[i] = 1
	}
}

func (p *processor) InitConsumption() {
	p.prepareStmt.Grow(300)
	p.prepareStmt.WriteString("select avg(fuel_consumption) as avg_fuel_consumption,avg(nominal_fuel_consumption) as nominal_fuel_consumption from benchmark.readings where velocity > 1 group by fleet")
}

func (p *processor) InitDuration() {
	p.prepareStmt.Grow(300)
	p.prepareStmt.WriteString("select name,min(fleet) as fleet,min(driver) as driver,avg(hours_driven) as avg_daily_hours from (select time_bucket(k_timestamp, '1d') as " +
		"k_timestamp, name,min(fleet) as fleet,min(driver) as driver,count(avg_v)/6 as hours_driven from (select time_bucket(k_timestamp, '600s') " +
		"as k_timestamp,name,last(fleet) as fleet ,last(driver) as driver,avg(velocity) as avg_v from benchmark.readings where " +
		"k_timestamp > $1 AND k_timestamp <= $2 group by name, time_bucket(k_timestamp, '600s') ) where  avg_v > 1  group by name, time_bucket(k_timestamp, '1d')) group by name ")
	p.formatBuf = make([]int16, 2)
	for i := 0; i < 2; i++ {
		p.formatBuf[i] = 1
	}
}

func (p *processor) InitSession() {
	p.prepareStmt.Grow(300)
	p.prepareStmt.WriteString(`select time_bucket(ts, '1d') as ts,name,avg(ela) from (SELECT ts,name,diff(difka)  
		OVER (PARTITION BY name order by ts) as dif ,diff(cast(ts as bigint)) OVER (PARTITION BY name order by ts) as ela  
        FROM (SELECT time_bucket(k_timestamp, '600s') as ts,name,diff(cast(cast(floor(avg(velocity)/5) as bool) as int))  
     	OVER (PARTITION BY name order by time_bucket(k_timestamp, '600s'))  AS difka FROM benchmark.readings   
        WHERE name is not null  AND  k_timestamp > $1 AND k_timestamp <= $2 group by name,time_bucket(k_timestamp, '600s'))  
		WHERE difka!=0)  WHERE dif = -2 group by name,time_bucket(ts, '1d')`)
	p.formatBuf = make([]int16, 2)
	for i := 0; i < 2; i++ {
		p.formatBuf[i] = 1
	}
}

func (p *processor) InitLoad() {
	p.prepareStmt.Grow(300)
	p.prepareStmt.WriteString("select fleet,model,load_capacity,avg(ml/load_capacity)  from (SELECT last(fleet) as fleet, last(model) as model,last(load_capacity) as load_capacity,avg(current_load) AS ml FROM benchmark.diagnostics  group BY name) group BY fleet, model,load_capacity")
}

func (p *processor) InitActivity() {
	p.prepareStmt.Grow(300)
	p.prepareStmt.WriteString("SELECT time_bucket(k_timestamp, '1d') as k_timestamp, model,fleet,count(avg_status)/144 FROM" +
		" (SELECT time_bucket(k_timestamp, '600s') as k_timestamp, last(model) as model, last(fleet) as fleet,avg(status) AS" +
		" avg_status FROM benchmark.diagnostics WHERE k_timestamp >= $1 AND k_timestamp < $2 group by  name,time_bucket(k_timestamp, '600s'))" +
		" WHERE avg_status<1 group by  model, fleet, time_bucket(k_timestamp, '1d')")
	p.formatBuf = make([]int16, 2)
	for i := 0; i < 2; i++ {
		p.formatBuf[i] = 1
	}
}

func (p *processor) InitFrequency() {
	p.prepareStmt.Grow(300)
	p.prepareStmt.WriteString(`SELECT model,count(state_changed) FROM (SELECT model,diff(broken_down)  OVER (PARTITION BY name order by ts)  
		AS state_changed FROM (SELECT time_bucket(k_timestamp, '600s') as ts,name,last(model) as model,cast(cast(floor(2*(sum(cast(cast(status as bool) as int))/count(status))) as bool) as int) 
		AS broken_down  FROM benchmark.diagnostics where k_timestamp  > $1 AND k_timestamp  <= $2 group by name,time_bucket(k_timestamp, '600s'))) WHERE state_changed = 1 group by model`)
	p.formatBuf = make([]int16, 2)
	for i := 0; i < 2; i++ {
		p.formatBuf[i] = 1
	}
}
func (p *processor) RunLastLoc(querys []string, tableBuffer *fixedArgList) {
	v := strings.TrimSpace(querys[0])
	tableBuffer.Append([]byte(v))
}

func (p *processor) RunStationaryTrucks(querys []string, tableBuffer *fixedArgList) {
	v := strings.TrimSpace(querys[0])
	tableBuffer.Append([]byte(v))
	//time
	settime(querys[1], querys[2], tableBuffer)
}

func (p *processor) RunLongDrivingSessions(querys []string, tableBuffer *fixedArgList) {
	v := strings.TrimSpace(querys[0])
	tableBuffer.Append([]byte(v))
	//time
	settime(querys[1], querys[2], tableBuffer)
}

func (p *processor) RunLongDailySessions(querys []string, tableBuffer *fixedArgList) {
	v := strings.TrimSpace(querys[0])
	tableBuffer.Append([]byte(v))
	//time
	settime(querys[1], querys[2], tableBuffer)
}

func (p *processor) Run(querys []string, tableBuffer *fixedArgList) {
}

func (p *processor) RunDuration(querys []string, tableBuffer *fixedArgList) {
	settime(querys[0], querys[1], tableBuffer)
}
