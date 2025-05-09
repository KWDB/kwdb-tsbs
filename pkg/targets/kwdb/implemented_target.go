package kwdb

import (
	"bytes"

	"github.com/blagojts/viper"
	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/pkg/data/serialize"
	"github.com/timescale/tsbs/pkg/data/source"
	"github.com/timescale/tsbs/pkg/targets"
	"github.com/timescale/tsbs/pkg/targets/constants"
)

func NewTarget() targets.ImplementedTarget {
	return &kwdbTarget{}
}

type kwdbTarget struct {
}

func (t *kwdbTarget) TargetSpecificFlags(flagPrefix string, flagSet *pflag.FlagSet) {
	flagSet.String(flagPrefix+"user", "root", "User to connect to kwdb")
	flagSet.String(flagPrefix+"pass", "", "Password for user connecting to kwdb")
	flagSet.String(flagPrefix+"host", "", "kwdb host")
	flagSet.Int(flagPrefix+"port", 26257, "kwdb client Port")
	flagSet.String(flagPrefix+"dbname", "benchmark", "kwdb db name")
	flagSet.String(flagPrefix+"insert-type", "9091", "kwdb insert type")
	flagSet.String(flagPrefix+"case", "cpu-only", "kwdb use-case")
	flagSet.Int(flagPrefix+"preparesize", 1000, "Prepare batch size ")
	flagSet.String(flagPrefix+"certdir", "", "Dir of cert files")
	flagSet.String(flagPrefix+"partition", "true", "alter table partition by hashpoint p0 p1 p2")
}

func (t *kwdbTarget) TargetName() string {
	return constants.FormatKwdb
}

func (t *kwdbTarget) Serializer() serialize.PointSerializer {
	return &Serializer{
		tableMap:   map[string]struct{}{},
		superTable: map[string]*Table{},
		tmpBuf:     &bytes.Buffer{},
	}
}

func (t *kwdbTarget) Benchmark(targetDB string, dataSourceConfig *source.DataSourceConfig, v *viper.Viper,
) (targets.Benchmark, error) {
	var loadingOptions LoadingOptions
	if err := v.Unmarshal(&loadingOptions); err != nil {
		return nil, err
	}
	return NewBenchmark(targetDB, &loadingOptions, dataSourceConfig)
}
