#!/bin/bash
# ==============================================
# KWDB-tsbs 基准参数配置脚本
# 说明：用于定义 KWDB 性能测试的默认参数
# 用法：使用命令行：`Parameter=Value ./tsbs_kwdb.sh`（例如：`workspace="/root" scale=4000 ./tsbs_kwdb.sh`）
# ==============================================

# -----------------------------------------------------------------------------
# 必需参数（如果未设置，将以错误终止）
# -----------------------------------------------------------------------------
: ${workspace:?Error: workspace must be specified!}  # KWDB 工作目录路径

# -----------------------------------------------------------------------------
# 可配置参数（带默认值）
# -----------------------------------------------------------------------------

## 测试规模配置
scale=${scale:-100}     # 设备数量
format=${format:-kwdb}  # 数据格式

## 查询测试配置
query_workers=${query_workers:-1}      # 查询并发线程数，默认为 1
query_times=${query_times:-1}          # 每种查询类型的执行次数，默认为 1
enable_perf=${enable_perf:-false}
parallel_degree=${parallel_degree:-8}  # 查询并行性，默认为 8

## 数据写入配置
insert_type=${insert_type:-insert}  # 写入方式，默认为 insert,可设置：insert、prepare、prepareiot
tsbs_case=${tsbs_case:-cpu-only}    # case 类型，默认为 cpu-only, 可设置：iot

    注：case 与 insert-type 的对应关系如下：
    | case     | insert-type |
    |----------|-------------|
    | cpu-only | insert      |
    | cpu-only | prepare     |
    | IoT      | insert      |
    | IoT      | prepareiot  |

prepare=${prepare:-false}           # 是否使用模板查询，默认为false
load_workers=${load_workers:-12}    # 并发数据加载次数，默认为 12

## 群集配置
node_num=${node_num:-1}                  # 当前节点号
cluster_node_num=${cluster_node_num:-1}  # 群集中的节点总数
ip=${ip:-127.0.0.1}                      # 节点IP地址
ports="${ports:-26257,26258,26259}"      # 监听端口号
httpports=${httpports:-8981,8982,8983}

## WAL和副本配置
wal=${wal:-0}                    # WAL 等级，默认为 0
replica_mode=${replica_mode:-1}  # 副本模式

enable_buffer_pool=${enable_buffer_pool:-true}   # 内存配置
ts_automatic_collection=${ts_automatic_collection:-false}
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

printf "%-20s %-20s %-20s %-20s\n" \
  "scale=${scale}" "format=${format}" "query_workers=${query_workers}" "query_times=${query_times}" "prepare=${prepare}"\
  "enable_perf=${enable_perf}" "insert_type=${insert_type}" "node_num=${node_num}" "cluster_node_num=${cluster_node_num}" \
  "wal=${wal}" "replica_mode=${replica_mode}" "ip=${ip}" "ports=${ports}" \
  "load_workers=${load_workers}" "parallel_degree=${parallel_degree}" "enable_buffer_pool=${enable_buffer_pool}" "httpports=${httpports}" \
  "ts_automatic_collection=${ts_automatic_collection}"
printf "\n"


data_dir=${workspace}/tsbs_test
cd ${workspace}/bin
export KW_WAL_LEVEL=${wal}

IFS=',' read -ra ports <<< "$ports"
IFS=',' read -ra httpports <<< "$httpports"
# 检查端口数量是否足够
if [ "${#ports[@]}" -lt "$node_num" ]|| [ "${#httpports[@]}" -lt "$node_num" ]; then
    echo "错误：ports或者httpports数量与node_num数量不匹配"
    exit 1
fi

echo "-----------server:${node_num}node mode${replica_mode}-----------"

for ((i=1;i<=${node_num};i++))
do
    listenport=$((${i}+26256))
    httpport=$((${i}+8980))
    store=${data_dir}/kwbase-data${i}
    rm -rf ${data_dir}/kwbase-data${i}
    sleep 10
    echo "#### Node${i} start ####"
    if [[ ${replica_mode} == 3 ]]; then
	if [ ${enable_buffer_pool} == "true" ]; then
        LD_LIBRARY_PATH=../lib ./kwbase start --insecure --listen-addr=${ip}:${ports[i-1]} --http-addr=${ip}:${httpports[i-1]} --store=$store --join=${ip}:${ports[0]} --log-file-verbosity=ERROR --buffer-pool-size=65314 --background
        else
	    	LD_LIBRARY_PATH=../lib ./kwbase start --insecure --listen-addr=${ip}:${ports[i-1]} --http-addr=${ip}:${httpports[i-1]} --store=$store --join=${ip}:${ports[0]} --log-file-verbosity=ERROR --background
	fi
    else
        if [[ ${cluster_node_num} == 1 ]]; then
	    if [ ${enable_buffer_pool} == "true" ]; then
              LD_LIBRARY_PATH=../lib ./kwbase start-single-node --insecure --listen-addr=${ip}:${ports[i-1]} --http-addr=${ip}:${httpports[i-1]}  --store=$store --log-file-verbosity=ERROR --buffer-pool-size=65314 --background
            else
                LD_LIBRARY_PATH=../lib ./kwbase start-single-node --insecure --listen-addr=${ip}:${ports[i-1]} --http-addr=${ip}:${httpports[i-1]}  --store=$store --log-file-verbosity=ERROR --background
	    fi
        else
	    if [ ${enable_buffer_pool} == "true" ]; then
                LD_LIBRARY_PATH=../lib ./kwbase start-single-replica --insecure --listen-addr=${ip}:${ports[i-1]} --http-addr=${ip}:${httpports[i-1]}  --store=$store  --join=${ip}:${ports[0]} --log-file-verbosity=ERROR --buffer-pool-size=65314 --background
	    else
                LD_LIBRARY_PATH=../lib ./kwbase start-single-replica --insecure --listen-addr=${ip}:${ports[i-1]} --http-addr=${ip}:${httpports[i-1]} --store=$store  --join=${ip}:${ports[0]} --log-file-verbosity=ERROR --background
	    fi
        fi
    fi
    sleep 5
done

echo "sleep 30s to wait all nodes start"
sleep 30
if [ ${node_num:-1} -gt 1 ]; then
  LD_LIBRARY_PATH=../lib ./kwbase init --insecure --host=${ip}:${ports[0]}
  sleep 10
fi

echo "check kwdb cluster node status"
LD_LIBRARY_PATH=../lib ./kwbase node status --insecure --host=${ip}:${ports[0]}
for ((i=1;i<=${cluster_node_num};i++))
do
    ((line=1+$i))
    node_status=$(LD_LIBRARY_PATH=../lib ./kwbase node status --insecure --host=${ip}:${ports[i-1]} | awk 'NR=='$line'{print $11}')
    node_ip=$(LD_LIBRARY_PATH=../lib ./kwbase node status --insecure --host=${ip}:${ports[i-1]} | awk 'NR=='$line'{split($2, ip_port, ":"); print ip_port[1]}')
    echo "node ${i} available status is ${node_status}"
    if [ $i -eq 1 ]; then
        ip1=${node_ip}
        echo "node ${i} ip${i} is ${node_ip}"
    fi
    if [ $i -eq 2 ]; then
        ip2=${node_ip}
        echo "node ${i} ip${i} is ${node_ip}"
    fi
    if [ $i -eq 3 ]; then
        ip3=${node_ip}
        echo "node ${i} ip${i} is ${node_ip}"
    fi
    if [ ${node_status} != "true" ]; then
        exit 1
    fi
done


LD_LIBRARY_PATH=../lib ./kwbase sql --insecure --host=${ip}:${ports[0]} --execute="set max_push_limit_number = 10000000;"
LD_LIBRARY_PATH=../lib ./kwbase sql --insecure --host=${ip}:${ports[0]} --execute="set can_push_sorter = true;"

# 设置开始
LD_LIBRARY_PATH=../lib ./kwbase sql --insecure --host=${ip}:${ports[0]} --execute="set cluster setting sql.distsql.temp_storage.workmem='4096Mib';"
sleep 5
LD_LIBRARY_PATH=../lib ./kwbase sql --insecure --host=${ip}:${ports[0]} --execute="set cluster setting sql.all_push_down.enabled=true;"
sleep 5
LD_LIBRARY_PATH=../lib ./kwbase sql --insecure --host=${ip}:${ports[0]} --execute="set cluster setting sql.pg_encode_short_circuit.enabled = true;"
sleep 5
LD_LIBRARY_PATH=../lib ./kwbase sql --insecure --host=${ip}:${ports[0]} --execute="set cluster setting ts.parallel_degree=${parallel_degree};"
sleep 5
LD_LIBRARY_PATH=../lib ./kwbase sql --insecure --host=${ip}:${ports[0]} --execute="set cluster setting sql.stats.ts_automatic_collection.enabled=${ts_automatic_collection};"
LD_LIBRARY_PATH=../lib ./kwbase sql --insecure --host=${ip}:${ports[0]} --execute="alter schedule scheduled_table_compress  Recurring  '0 0 1 1 ? 2099';"
LD_LIBRARY_PATH=../lib ./kwbase sql --insecure --host=${ip}:${ports[0]} --execute="alter schedule scheduled_table_retention  Recurring  '0 0 1 1 ? 2099';"
LD_LIBRARY_PATH=../lib ./kwbase sql --insecure --host=${ip}:${ports[0]} --execute="set cluster setting server.tsinsert_direct.enabled = true;"
LD_LIBRARY_PATH=../lib ./kwbase sql --insecure --host=${ip}:${ports[0]} --execute="set cluster setting ts.dedup.rule=keep;"
LD_LIBRARY_PATH=../lib ./kwbase sql --insecure --host=${ip}:${ports[0]} --execute="set cluster setting sql.stats.tag_automatic_collection.enabled = false;"
sleep 5
LD_LIBRARY_PATH=../lib ./kwbase sql --insecure --host=${ip}:${ports[0]} --execute="set cluster setting ts.ack_before_application.enabled=true;"
# 设置结束


if [ ${format} != kwdb ]; then
    echo "unsupported format"
    exit 1
fi

kwdb_ip=$(ps -ef | grep kwbase | awk 'NR==1{print $11}' | awk -F ':' '{print $1}' | awk -F '=' '{print $2}')
kwdb_port=$(ps -ef | grep kwbase | awk 'NR==1{print $11}' | awk -F ':' '{print $2}')
echo "kwdb me_ip="${kwdb_ip}" me_port="${kwdb_port}""

##case 类型
##[cpu-only | iot ]
#tsbs_case="cpu-only"

time=`date +%Y_%m%d_%H%M%S`

#数据和结果根路径
loadDataDir=${SCRIPT_DIR}/../load_data
loadResultDir=${SCRIPT_DIR}/../reports/${time}_scale${scale}_cluster${cluster_node_num}_insertdirect${insert_direct}_${insert_type}_wal${wal}_replica${replica_mode}_dop${parallel_degree}/load_result
queryDataDir=${SCRIPT_DIR}/../query_data/scale${scale}
queryResultDir=${SCRIPT_DIR}/../reports/${time}_scale${scale}_cluster${cluster_node_num}_insertdirect${insert_direct}_${insert_type}_wal${wal}_replica${replica_mode}_dop${parallel_degree}/query_result

mkdir -p ${loadDataDir}
mkdir -p ${loadResultDir}
mkdir -p ${queryDataDir}
mkdir -p ${queryResultDir}

# 数据库名称
db_name="benchmark"

# 负载测试参数
load_ts_start="2016-01-01T00:00:00Z"
load_ts_end="2016-02-01T00:00:00Z"
if [ ${scale} -eq 100 ]; then
    load_ts_end="2016-02-01T00:00:00Z"
    echo "generate 31 days data"
elif [ ${scale} -eq 4000 ]; then
    load_ts_end="2016-01-05T00:00:00Z"
    echo "generate 4 days data"
elif [ ${scale} -eq 100000 ]; then
    load_ts_end="2016-01-01T03:00:00Z"
    echo "generate 3 hours data"
elif [ ${scale} -eq 1000000 ]; then
    LD_LIBRARY_PATH=../lib ./kwbase sql --insecure --host=${ip}:${ports[0]} --execute="SET CLUSTER SETTING ts.entities_per_subgroup.max_limit=5000;"
    LD_LIBRARY_PATH=../lib ./kwbase sql --insecure --host=${ip}:${ports[0]} --execute="SET CLUSTER SETTING ts.blocks_per_segment.max_limit=5000;"
    LD_LIBRARY_PATH=../lib ./kwbase sql --insecure --host=${ip}:${ports[0]} --execute="SET CLUSTER SETTING ts.rows_per_block.max_limit=18;"
    load_ts_end="2016-01-01T00:03:00Z"
    echo "generate 3 minutes data"
elif [ ${scale} -eq 10000000 ]; then
    LD_LIBRARY_PATH=../lib ./kwbase sql --insecure --host=${ip}:${ports[0]} --execute="SET CLUSTER SETTING ts.entities_per_subgroup.max_limit=5000;"
    LD_LIBRARY_PATH=../lib ./kwbase sql --insecure --host=${ip}:${ports[0]} --execute="SET CLUSTER SETTING ts.blocks_per_segment.max_limit=5000;"
    LD_LIBRARY_PATH=../lib ./kwbase sql --insecure --host=${ip}:${ports[0]} --execute="SET CLUSTER SETTING ts.rows_per_block.max_limit=18;"
    load_ts_end="2016-01-01T00:03:00Z"
    echo "generate 3 minutes data"
elif [ ${scale} -eq 20000000 ]; then
    LD_LIBRARY_PATH=../lib ./kwbase sql --insecure --host=${ip}:${ports[0]} --execute="SET CLUSTER SETTING ts.entities_per_subgroup.max_limit=5000;"
    LD_LIBRARY_PATH=../lib ./kwbase sql --insecure --host=${ip}:${ports[0]} --execute="SET CLUSTER SETTING ts.blocks_per_segment.max_limit=5000;"
    LD_LIBRARY_PATH=../lib ./kwbase sql --insecure --host=${ip}:${ports[0]} --execute="SET CLUSTER SETTING ts.rows_per_block.max_limit=100;"
    load_ts_end="2016-01-01T00:16:40Z"
    echo "generate 3 minutes data"
else
    echo "generate 31 days data"
fi

load_batchsizes="1000"
load_interval="10s"

# 查询测试参数
query_ts_start="2016-01-01T00:00:00Z"
query_ts_end="2016-01-05T00:00:01Z"

if [ ${scale} -eq 100000 ]; then
   query_ts_end="2016-01-01T15:00:01Z"
elif [ ${scale} -eq 1000000 ] || [ ${scale} -eq 10000000 ]; then
   query_ts_end="2016-01-01T12:04:01Z"
else
   echo "use defalut query end time"
fi

CPUONLY_QUERY_TYPES_ALL="\
single-groupby-1-1-1 \
single-groupby-1-1-12 \
single-groupby-1-8-1 \
single-groupby-5-1-1 \
single-groupby-5-1-12 \
single-groupby-5-8-1 \
cpu-max-all-1 \
cpu-max-all-8 \
double-groupby-1 \
double-groupby-5 \
double-groupby-all \
high-cpu-all \
high-cpu-1 \
lastpoint \
groupby-orderby-limit"

IOT_QUERY_TYPES_ALL="\
last-loc \
single-last-loc \
low-fuel \
high-load \
stationary-trucks \
long-driving-sessions \
long-daily-sessions \
avg-vs-projected-fuel-consumption \
avg-daily-driving-duration \
avg-daily-driving-session \
avg-load \
daily-activity \
breakdown-frequency"


# 生成导入数据
if [ ! -f "${loadDataDir}/${tsbs_case}_${format}_scale_${scale}_${load_workers}order.dat" ]; then
    echo start to generate load data
    cd "$SCRIPT_DIR/../bin/"
    ./tsbs_generate_data \
        --format=${format} \
        --use-case=${tsbs_case} \
        --seed=123 \
        --scale=${scale} \
        --log-interval=${load_interval} \
        --timestamp-start=${load_ts_start} \
        --timestamp-end=${load_ts_end} \
        --orderquantity=${load_workers} > ${loadDataDir}/${tsbs_case}_${format}_scale_${scale}_${load_workers}order.dat
    sleep 10
else
    echo load data already exists
fi


if [ ${tsbs_case} == "cpu-only" ]; then
    QUERY_TYPES_ALL=${CPUONLY_QUERY_TYPES_ALL}
elif [ ${tsbs_case} == "iot" ]; then
    QUERY_TYPES_ALL=${IOT_QUERY_TYPES_ALL}
fi

echo "start to generate query data"
for QUERY_TYPE in ${QUERY_TYPES_ALL}; do
if [ ! -f "${queryDataDir}/${format}_scale${scale}_${tsbs_case}_${QUERY_TYPE}_query_times${query_times}.dat" ]; then
     cd "$SCRIPT_DIR/../bin/"
    ./tsbs_generate_queries \
    --format=${format} \
    --use-case=${tsbs_case} \
    --seed=123 \
    --scale=${scale} \
    --query-type=${QUERY_TYPE} \
    --queries=${query_times} \
    --timestamp-start=${query_ts_start} \
    --timestamp-end=${query_ts_end} \
    --prepare=${prepare}\
    --db-name=${db_name} | gzip > ${queryDataDir}/${format}_scale${scale}_${tsbs_case}_${QUERY_TYPE}_query_times${query_times}.dat.gz

    gunzip ${queryDataDir}/${format}_scale${scale}_${tsbs_case}_${QUERY_TYPE}_query_times${query_times}.dat.gz
    sleep 5
else
    echo ${QUERY_TYPE} query data already exists
fi
done

partition=true
if [[ ${cluster_node_num} == 1 ]]; then
    partition=false
fi

echo "start to load data $format with workers ${load_workers}"
  cd "$SCRIPT_DIR/../bin/"
  ./tsbs_load_kwdb \
  --file=${loadDataDir}/${tsbs_case}_${format}_scale_${scale}_${load_workers}order.dat \
  --user=root \
  --pass=1234 \
  --host=${kwdb_ip} \
  --port=${kwdb_port} \
  --insert-type=${insert_type} \
  --db-name=${db_name} \
  --batch-size=${load_batchsizes} \
  --case=${tsbs_case} \
  --partition=${partition} \
  --workers=${load_workers} > ${loadResultDir}/${tsbs_case}_${format}_scale_${scale}.log
echo "load data success"
sleep 10

cd ${workspace}/bin
LD_LIBRARY_PATH=../lib ./kwbase sql --insecure --host=${ip}:${ports[0]} --execute="select * from kwdb_internal.ranges where table_name='cpu';" > ${queryResultDir}/ranges_info.log
echo "write range info success"


echo "start to run queries worker 1"
query_workers=1
for QUERY_TYPE in ${QUERY_TYPES_ALL}; do
   cd "$SCRIPT_DIR/../bin/"
   ./tsbs_run_queries_kwdb \
   --file=${queryDataDir}/${format}_scale${scale}_${tsbs_case}_${QUERY_TYPE}_query_times${query_times}.dat \
   --user=root \
   --pass=1234 \
   --host=${kwdb_ip} \
   --port=${kwdb_port} \
   --prepare=${prepare}\
   --workers=${query_workers}\
   --query-type=${QUERY_TYPE} > ${queryResultDir}/${format}_scale${scale}_${tsbs_case}_${QUERY_TYPE}_worker1.log

   sleep 10
   echo "run query ${QUERY_TYPE} worker 1 success"
done

echo "start to run queries worker 8"
query_workers=8
for QUERY_TYPE in ${QUERY_TYPES_ALL}; do
   cd "$SCRIPT_DIR/../bin/"
   ./tsbs_run_queries_kwdb \
   --file=${queryDataDir}/${format}_scale${scale}_${tsbs_case}_${QUERY_TYPE}_query_times${query_times}.dat \
   --user=root \
   --pass=1234 \
   --host=${kwdb_ip} \
   --port=${kwdb_port} \
    --prepare=${prepare}\
   --workers=${query_workers} \
   --query-type=${QUERY_TYPE} > ${queryResultDir}/${format}_scale${scale}_${tsbs_case}_${QUERY_TYPE}_worker8.log

   sleep 10
   echo "run query ${QUERY_TYPE} worker 8 success"
done

pkill -9 kwbase
sleep 10
