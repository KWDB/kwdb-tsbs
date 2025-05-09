module github.com/timescale/tsbs

go 1.14

require (
	github.com/HdrHistogram/hdrhistogram-go v1.1.2
	github.com/SiriDB/go-siridb-connector v0.0.0-20190110105621-86b34c44c921
	github.com/StackExchange/wmi v1.2.1 // indirect
	github.com/andreyvit/diff v0.0.0-20170406064948-c7f18ee00883
	github.com/aws/aws-sdk-go v1.44.20
	github.com/blagojts/viper v1.6.3-0.20200313094124-068f44cf5e69
	github.com/globalsign/mgo v0.0.0-20181015135952-eeefdecb41b8
	github.com/gocql/gocql v0.0.0-20211222173705-d73e6b1002a7
	github.com/golang/protobuf v1.5.4
	github.com/golang/snappy v0.0.4
	github.com/google/flatbuffers v23.5.26+incompatible
	github.com/google/go-cmp v0.7.0
	github.com/jackc/pgx/v4 v4.18.2
	github.com/jackc/pgx/v5 v5.5.4
	github.com/jmoiron/sqlx v1.2.1-0.20190826204134-d7d95172beb5
	github.com/kshvakov/clickhouse v1.3.11
	github.com/lib/pq v1.10.2
	github.com/pkg/errors v0.9.1
	github.com/prometheus/common v0.37.0
	github.com/shirou/gopsutil v3.21.8+incompatible
	github.com/silenceper/pool v1.0.0
	github.com/spf13/cobra v1.5.0
	github.com/spf13/pflag v1.0.5
	github.com/timescale/promscale v0.0.0-20221221142019-a7a8f7c4de78
	github.com/transceptor-technology/go-qpack v0.0.0-20190116123619-49a14b216a45
	github.com/valyala/fasthttp v1.53.0
	go.uber.org/atomic v1.10.0
	golang.org/x/net v0.38.0
	golang.org/x/time v0.5.0
	gopkg.in/yaml.v2 v2.4.0
)

replace (
	github.com/containerd/containerd => github.com/containerd/containerd v1.6.38
	github.com/docker/distribution => github.com/docker/distribution v2.8.2-beta.1+incompatible
	github.com/docker/docker => github.com/docker/docker v25.0.6+incompatible
	github.com/elazarl/goproxy => github.com/elazarl/goproxy v0.0.0-20230731152917-f99041a5c027
	github.com/gin-gonic/gin => github.com/gin-gonic/gin v1.10.0
	github.com/go-jose/go-jose/v3 => github.com/go-jose/go-jose/v3 v3.0.4
	github.com/gogo/protobuf => github.com/gogo/protobuf v1.3.2
	github.com/golang/glog => github.com/golang/glog v1.2.4
	github.com/hashicorp/consul/api => github.com/hashicorp/consul/api v1.28.2
	github.com/hashicorp/go-retryablehttp => github.com/hashicorp/go-retryablehttp v0.7.7
	github.com/jackc/pgproto3/v2 => github.com/jackc/pgproto3/v2 v2.3.3
	github.com/jackc/pgx/v4 => github.com/jackc/pgx/v4 v4.18.2
	github.com/jackc/pgx/v5 v5.5.4 => ./pgx/v5
	github.com/kataras/iris/v12 => github.com/kataras/iris/v12 v12.2.11
	github.com/labstack/echo/v4 => github.com/labstack/echo/v4 v4.9.0
	github.com/mattn/go-sqlite3 => github.com/mattn/go-sqlite3 v1.14.18
	github.com/opencontainers/runc => github.com/opencontainers/runc v1.1.12
	github.com/pkg/sftp => github.com/pkg/sftp v1.13.9
	github.com/prometheus/client_golang => github.com/prometheus/client_golang v1.11.1
	github.com/sirupsen/logrus => github.com/sirupsen/logrus v1.8.3
	github.com/spf13/cobra => github.com/spf13/cobra v1.5.0
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp => go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.60.0
	golang.org/x/crypto => golang.org/x/crypto v0.35.0
	golang.org/x/net => golang.org/x/net v0.38.0
	golang.org/x/text => golang.org/x/text v0.3.8
	google.golang.org/grpc => google.golang.org/grpc v1.56.3
	google.golang.org/protobuf => google.golang.org/protobuf v1.34.1
	gopkg.in/yaml.v2 => gopkg.in/yaml.v2 v2.2.8
	k8s.io/client-go => k8s.io/client-go v0.30.6
)
