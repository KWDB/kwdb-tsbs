package kwdb

import (
	"fmt"
	"strings"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/iot"
	"github.com/timescale/tsbs/pkg/query"
)

// IoT produces KWDB-specific queries for all the iot query types.
type IoT struct {
	*iot.Core
	*BaseGenerator
}

//last-loc
//single-last-loc
//low-fuel
//avg-vs-projected-fuel-consumption
//avg-daily-driving-duration
//daily-activity

func (i *IoT) getTrucksWhereWithNames(names []string) string {
	var nameClauses []string

	for _, s := range names {
		nameClauses = append(nameClauses, fmt.Sprintf("'%s'", s))
	}
	return fmt.Sprintf("name IN (%s)", strings.Join(nameClauses, ","))
}

// getHostWhereString gets multiple random hostnames and creates a WHERE SQL statement for these hostnames.
func (i *IoT) getTruckWhereString(nTrucks int) string {
	names, err := i.GetRandomTrucks(nTrucks)
	panicIfErr(err)
	return i.getTrucksWhereWithNames(names)
}

// LastLocByTruck finds the truck location for nTrucks.
func (i *IoT) LastLocByTruck(qi query.Query, nTrucks int) {
	var prepare bool
	if kaiwudb, ok := qi.(*query.Kwdb); ok {
		prepare = kaiwudb.GetPrepare()
	}
	var sql string
	if !prepare {
		sql = fmt.Sprintf(`SELECT last(k_timestamp), last(latitude), last(longitude) FROM %s.readings WHERE %s GROUP BY name`,
			i.ReadingDBName,
			i.getTruckWhereString(nTrucks))
	} else {
		sql = fmt.Sprintf(`truck_%d`, nTrucks)
	}

	humanLabel := "KWDB last location by specific truck"
	humanDesc := fmt.Sprintf("%s: random %4d trucks", humanLabel, nTrucks)

	i.fillInQuery(qi, humanLabel, humanDesc, iot.ReadingsTableName, sql)
}

// LastLocPerTruck finds all the truck locations along with truck and driver names.
func (i *IoT) LastLocPerTruck(qi query.Query) {
	var prepare bool
	if kaiwudb, ok := qi.(*query.Kwdb); ok {
		prepare = kaiwudb.GetPrepare()
	}
	var sql string
	if !prepare {
		sql = fmt.Sprintf(`SELECT last(k_timestamp), name, last(driver), last(latitude) as latitude, last(longitude) as longitude FROM %s.readings WHERE fleet='%s' and name IS NOT NULL GROUP BY name`,
			i.ReadingDBName, i.GetRandomFleet())
	} else {
		sql = fmt.Sprintf(`%s`, i.GetRandomFleet())
	}

	humanLabel := "KWDB last location per truck"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, iot.ReadingsTableName, sql)
}

// TrucksWithLowFuel finds all trucks with low fuel (less than 10%).
func (i *IoT) TrucksWithLowFuel(qi query.Query) {
	var prepare bool
	if kaiwudb, ok := qi.(*query.Kwdb); ok {
		prepare = kaiwudb.GetPrepare()
	}
	var sql string
	if !prepare {
		sql = fmt.Sprintf(`SELECT last_row(k_timestamp) ,name, last(driver) as driver ,last(fuel_state) as fuel_state FROM %s.diagnostics where fleet = '%s' and fuel_state <= 0.1   GROUP BY name`,
			i.ReadingDBName, i.GetRandomFleet())
	} else {
		sql = fmt.Sprintf(`%s`, i.GetRandomFleet())
	}

	humanLabel := "KWDB trucks with low fuel"
	humanDesc := fmt.Sprintf("%s: under 10 percent", humanLabel)

	i.fillInQuery(qi, humanLabel, humanDesc, iot.DiagnosticsTableName, sql)
}

// TrucksWithHighLoad finds all trucks that have load over 90%.
func (i *IoT) TrucksWithHighLoad(qi query.Query) {
	var prepare bool
	if kaiwudb, ok := qi.(*query.Kwdb); ok {
		prepare = kaiwudb.GetPrepare()
	}
	var sql string
	if !prepare {
		sql = fmt.Sprintf("SELECT ts,name,driver,current_load,load_capacity FROM (SELECT last_row(k_timestamp) as ts,name,last(driver) as driver , last(current_load) as current_load,last(load_capacity) as  load_capacity FROM  %s.diagnostics WHERE fleet = '%s' group by name) WHERE current_load>= (0.9 * load_capacity)", i.ReadingDBName, i.GetRandomFleet())
	} else {
		sql = fmt.Sprintf(`%s`, i.GetRandomFleet())
	}

	humanLabel := "KWDB trucks with high load"
	humanDesc := fmt.Sprintf("%s: over 90 percent", humanLabel)

	i.fillInQuery(qi, humanLabel, humanDesc, iot.DiagnosticsTableName, sql)
}

// StationaryTrucks finds all trucks that have low average velocity in a time window.
func (i *IoT) StationaryTrucks(qi query.Query) {
	var prepare bool
	if kaiwudb, ok := qi.(*query.Kwdb); ok {
		prepare = kaiwudb.GetPrepare()
	}
	var sql string
	interval := i.Interval.MustRandWindow(iot.StationaryDuration)
	if !prepare {
		sql = fmt.Sprintf("select name,driver from (SELECT name,driver,avg(velocity) as mean_velocity from  %s.readings  where fleet = '%s' and k_timestamp > '%s' AND k_timestamp <= '%s' group BY name,driver) WHERE mean_velocity < 1", i.ReadingDBName, i.GetRandomFleet(), parseTime(time.UnixMilli(interval.StartUnixMillis()).UTC()), parseTime(time.UnixMilli(interval.EndUnixMillis()).UTC()))
	} else {
		sql = fmt.Sprintf(`%s,%d,%d`, i.GetRandomFleet(), interval.StartUnixMillis(), interval.EndUnixMillis())
	}

	humanLabel := "KWDB stationary trucks"
	humanDesc := fmt.Sprintf("%s: with low avg velocity in last 10 minutes", humanLabel)

	i.fillInQuery(qi, humanLabel, humanDesc, iot.ReadingsTableName, sql)
}

// TrucksWithLongDrivingSessions finds all trucks that have not stopped at least 20 mins in the last 4 hours.
func (i *IoT) TrucksWithLongDrivingSessions(qi query.Query) {
	var prepare bool
	if kaiwudb, ok := qi.(*query.Kwdb); ok {
		prepare = kaiwudb.GetPrepare()
	}
	var sql string
	interval := i.Interval.MustRandWindow(iot.LongDrivingSessionDuration)
	if !prepare {
		sql = fmt.Sprintf("SELECT name,driver FROM (SELECT name,last(driver) as driver,avg(velocity) as mean_velocity FROM %s.readings WHERE fleet = '%s' AND k_timestamp > '%s' AND k_timestamp <= '%s' group BY name, time_bucket(k_timestamp, '600s')) WHERE mean_velocity > 1 GROUP BY name,driver having count(*) > %d", i.ReadingDBName, i.GetRandomFleet(), parseTime(time.UnixMilli(interval.StartUnixMillis()).UTC()), parseTime(time.UnixMilli(interval.EndUnixMillis()).UTC()), tenMinutePeriods(5, iot.LongDrivingSessionDuration))
	} else {
		sql = fmt.Sprintf("%s,%d,%d", i.GetRandomFleet(), interval.StartUnixMillis(), interval.EndUnixMillis())
	}

	humanLabel := "KWDB trucks with longer driving sessions"
	humanDesc := fmt.Sprintf("%s: stopped less than 20 mins in 4 hour period", humanLabel)

	i.fillInQuery(qi, humanLabel, humanDesc, iot.ReadingsTableName, sql)
}

// TrucksWithLongDailySessions finds all trucks that have driven more than 10 hours in the last 24 hours.
func (i *IoT) TrucksWithLongDailySessions(qi query.Query) {
	var prepare bool
	if kaiwudb, ok := qi.(*query.Kwdb); ok {
		prepare = kaiwudb.GetPrepare()
	}
	var sql string
	interval := i.Interval.MustRandWindow(iot.DailyDrivingDuration)
	if !prepare {
		sql = fmt.Sprintf("SELECT name,last(driver) as driver FROM %s.readings WHERE fleet ='%s' AND k_timestamp > '%s' AND k_timestamp <= '%s'   group BY name having count(*) > 60 and avg(velocity) > 1 ", i.ReadingDBName, i.GetRandomFleet(), parseTime(time.UnixMilli(interval.StartUnixMillis()).UTC()), parseTime(time.UnixMilli(interval.EndUnixMillis()).UTC()))
	} else {
		sql = fmt.Sprintf("%s,%d,%d", i.GetRandomFleet(), interval.StartUnixMillis(), interval.EndUnixMillis())
	}

	humanLabel := "KWDB trucks with longer daily sessions"
	humanDesc := fmt.Sprintf("%s: drove more than 10 hours in the last 24 hours", humanLabel)

	i.fillInQuery(qi, humanLabel, humanDesc, iot.ReadingsTableName, sql)
}

// AvgVsProjectedFuelConsumption calculates average and projected fuel consumption per fleet.
func (i *IoT) AvgVsProjectedFuelConsumption(qi query.Query) {
	sql := fmt.Sprintf("select avg(fuel_consumption) as avg_fuel_consumption,avg(nominal_fuel_consumption) as nominal_fuel_consumption from %s.readings where velocity > 1 group by fleet", i.ReadingDBName)
	humanLabel := "KWDB average vs projected fuel consumption per fleet"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, iot.ReadingsTableName, sql)
}

// AvgDailyDrivingDuration finds the average driving duration per driver.
func (i *IoT) AvgDailyDrivingDuration(qi query.Query) {
	var prepare bool
	if kaiwudb, ok := qi.(*query.Kwdb); ok {
		prepare = kaiwudb.GetPrepare()
	}
	var sql string
	if !prepare {
		sql = fmt.Sprintf("select name,min(fleet) as fleet,min(driver) as driver,avg(hours_driven) as avg_daily_hours from (select time_bucket(k_timestamp, '1d') as k_timestamp, name,min(fleet) as fleet,min(driver) as driver,count(avg_v)/6 as hours_driven from (select time_bucket(k_timestamp, '600s') as k_timestamp,name,last(fleet) as fleet ,last(driver) as driver,avg(velocity) as avg_v from %s.readings where k_timestamp > '%s' AND k_timestamp <= '%s' group by name, time_bucket(k_timestamp, '600s') ) where  avg_v > 1  group by name, time_bucket(k_timestamp, '1d')) group by name", i.ReadingDBName, parseTime(time.UnixMilli(i.Interval.StartUnixMillis()).UTC()), parseTime(time.UnixMilli(i.Interval.EndUnixMillis()).UTC()))
	} else {
		sql = fmt.Sprintf(`%d,%d`, i.Interval.StartUnixMillis(), i.Interval.EndUnixMillis())
	}
	humanLabel := "KWDB average driver driving duration per day"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, iot.ReadingsTableName, sql)
}

// AvgDailyDrivingSession finds the average driving session without stopping per driver per day.
func (i *IoT) AvgDailyDrivingSession(qi query.Query) {
	var prepare bool
	if kaiwudb, ok := qi.(*query.Kwdb); ok {
		prepare = kaiwudb.GetPrepare()
	}
	var sql string
	if !prepare {
		sql = fmt.Sprintf(`select time_bucket(ts, '1d') as ts,name,avg(ela) from (SELECT ts,name,diff(difka)  
		OVER (PARTITION BY name order by ts) as dif ,diff(cast(ts as bigint)) OVER (PARTITION BY name order by ts) as ela  
        FROM (SELECT time_bucket(k_timestamp, '600s') as ts,name,diff(cast(cast(floor(avg(velocity)/5) as bool) as int))  
     	OVER (PARTITION BY name order by time_bucket(k_timestamp, '600s'))  AS difka FROM %s.readings   
        WHERE name is not null  AND  k_timestamp > '%s' AND k_timestamp <= '%s' group by name,time_bucket(k_timestamp, '600s'))  
		WHERE difka!=0)  WHERE dif = -2 group by name,time_bucket(ts, '1d')`,
			i.ReadingDBName, parseTime(time.UnixMilli(i.Interval.StartUnixMillis()).UTC()), parseTime(time.UnixMilli(i.Interval.EndUnixMillis()).UTC()))
	} else {
		sql = fmt.Sprintf(`%d,%d`, i.Interval.StartUnixMillis(), i.Interval.EndUnixMillis())
	}
	humanLabel := "KWDB average driver driving session without stopping per day"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, iot.ReadingsTableName, sql)
}

// AvgLoad finds the average load per truck model per fleet.
func (i *IoT) AvgLoad(qi query.Query) {
	sql := fmt.Sprintf("select fleet,model,load_capacity,avg(ml/load_capacity)  from (SELECT last(fleet) as fleet, last(model) as model,last(load_capacity) as load_capacity,avg(current_load) AS ml FROM %s.diagnostics  group BY name) group BY fleet, model,load_capacity", i.ReadingDBName)

	humanLabel := "KWDB average load per truck model per fleet"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, iot.ReadingsTableName, sql)
}

// DailyTruckActivity returns the number of hours trucks has been active (not out-of-commission) per day per fleet per model.
func (i *IoT) DailyTruckActivity(qi query.Query) {
	var prepare bool
	if kaiwudb, ok := qi.(*query.Kwdb); ok {
		prepare = kaiwudb.GetPrepare()
	}
	var sql string
	if !prepare {
		sql = fmt.Sprintf("SELECT time_bucket(k_timestamp, '1d') as k_timestamp, model,fleet,count(avg_status)/144 FROM (SELECT time_bucket(k_timestamp, '600s') as k_timestamp, last(model) as model, last(fleet) as fleet,avg(status) AS avg_status FROM %s.diagnostics WHERE k_timestamp >= '%s' AND k_timestamp < '%s' group by  name,time_bucket(k_timestamp, '600s')) WHERE avg_status<1 group by  model, fleet, time_bucket(k_timestamp, '1d')", i.ReadingDBName, parseTime(time.UnixMilli(i.Interval.StartUnixMillis()).UTC()), parseTime(time.UnixMilli(i.Interval.EndUnixMillis()).UTC()))
	} else {
		sql = fmt.Sprintf(`%d,%d`, i.Interval.StartUnixMillis(), i.Interval.EndUnixMillis())
	}
	humanLabel := "KWDB daily truck activity per fleet per model"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, iot.ReadingsTableName, sql)
}

// TruckBreakdownFrequency calculates the amount of times a truck model broke down in the last period.
func (i *IoT) TruckBreakdownFrequency(qi query.Query) {
	var prepare bool
	if kaiwudb, ok := qi.(*query.Kwdb); ok {
		prepare = kaiwudb.GetPrepare()
	}
	var sql string
	if !prepare {
		sql = fmt.Sprintf(`SELECT model,count(state_changed) FROM (SELECT model,diff(broken_down)  OVER (PARTITION BY name order by ts)  
		AS state_changed FROM (SELECT time_bucket(k_timestamp, '600s') as ts,name,last(model) as model,cast(cast(floor(2*(sum(cast(cast(status as bool) as int))/count(status))) as bool) as int) 
		AS broken_down  FROM %s.diagnostics where k_timestamp  > '%s' AND k_timestamp  <= '%s' group by name,time_bucket(k_timestamp, '600s'))) WHERE state_changed = 1 group by model`,
			i.DiagnosticsDBName,
			parseTime(time.UnixMilli(i.Interval.StartUnixMillis()).UTC()), parseTime(time.UnixMilli(i.Interval.EndUnixMillis()).UTC()))
	} else {
		sql = fmt.Sprintf(`%d,%d`, i.Interval.StartUnixMillis(), i.Interval.EndUnixMillis())
	}
	humanLabel := "KWDB truck breakdown frequency per model"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, iot.ReadingsTableName, sql)
}

func tenMinutePeriods(minutesPerHour float64, duration time.Duration) int {
	durationMinutes := duration.Minutes()
	leftover := minutesPerHour * duration.Hours()
	return int((durationMinutes - leftover) / 10)
}

func parseTime(time time.Time) string {
	timeStr := strings.Split(time.String(), " ")
	return fmt.Sprintf("%s %s", timeStr[0], timeStr[1])
}
