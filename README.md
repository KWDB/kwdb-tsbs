# KWDB-TSBS

## Overview
kwdb-tsbs is an open-source high-performance time-series database benchmarking tool adapted from Timescale/tsbs, 
providing complete support for data generation, loading, and query testing specifically tailored for KWDB.

## Key Features
- **Data Generation**: Customizable device count, time range, and sampling interval
- **Efficient Data Loading**: Optimized bulk loading tool for KWDB
- **Query Benchmarks**: Standard time-series query templates
- **Automated Testing**: One-click end-to-end benchmarking workflow

## Building from Source
1. Clone repository
```bash
git clone https://gitee.com/kwdb/kwdb-tsbs.git
GOPATH/src/gitee.com/kwdb/kwdb-tsbs
```

2. Build application
```bash
make
```

Generated Binaries
```bash
kwdb-tsbs/
└── bin/
    ├── tsbs_generate_data      # Data generator
    ├── tsbs_load_kwdb          # Data loader
    ├── tsbs_generate_queries   # Query generator
    └── tsbs_run_queries_kwdb   # Query executor
```

## Automated Testing Script
* Recommended configuration

| component | Specification        |
|:----------|:---------------------|
| CPU       | 16-core              |
| Memory    | 32 GB                |
| Storage   | SSD                  |
| OS        | ubuntu20.04   x86_64 |
* Script location
```bash
kwdb-tsbs/scripts/tsbs_kwdb.sh
```

* Execute script
 ```bash
workspace="$GOPATH/src/gitee.com/kwdb" scripts/tsbs_kwdb.sh
```
* After the operation is completed, the relevant files are generated as follows
```bash
kwdb-tsbs/
├── load_data/          # Generated imported data
├── query_data/         # Generated query data
└── reports/            # Test result
    └── YMD_HMS_scale[scaleNum]_cluster[clusterNum]_insert[insertType]_wal[walSetting]_replica[replicaNum]_dop[degreeOfParallelism]/
        ├── load_data/  # Insert the results of the test
        └── query_data/ # Query the results of the test
```

* Notes
  * Ensure kwdb-tsbs and all dependencies are properly installed
  * Grant execute permission: chmod +x scripts/tsbs_kwdb.sh
  * Refer to script comments for parameter configurations
  * After the test is completed, a reports file will be generated in the kwdb-tsbs directory to record the test results

## Documentation
* KWDB Technical Specifications - Detailed parameters and usage guide:<br>
  kwdb_zh.md [(supplemental docs)](docs/kwdb_zh.md)
* Generic Benchmarking Framework - Cross-database testing methodology<br>
  README.md [(supplemental docs)](docs/README.md)

## License
This project is developed based on [timescale/tsbs](https://github.com/timescale/tsbs)  under MIT License, retain the original project copyright statement and extend the MIT license.

## Core Enhancements
* New Features
  * Full KWDB support implementation
  * Automated testing scripts for KWDB

* Documentation
  * Added bilingual (Chinese/English) README documentation