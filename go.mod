// fork 自 github.com/smallnest/rpcx，自 v3 起迁移为自有 module path，
// 使下游（tgf 及业务方）可直接 require 本 fork，无需 go.mod replace。
module github.com/thkhxm/rpcx/v2

go 1.26.0

toolchain go1.26.4

require (
	github.com/ChimeraCoder/gojson v1.1.0
	github.com/akutz/memconn v0.1.0
	github.com/alitto/pond v1.9.2
	github.com/apache/thrift v0.21.0
	github.com/edwingeng/doublejump v1.0.1
	github.com/fatih/color v1.18.0
	// go-echarts 钉在 v2.3.3：statsview v1.0.1 与 v2.4.x 不兼容
	// （v2.4.x 把 opts 的 bool 字段改为 types.Bool，statsview 编译失败）。
	// 用 require 降版而非 replace，保证对下游消费方传播。
	github.com/go-echarts/go-echarts/v2 v2.3.3
	github.com/go-ping/ping v1.2.0
	github.com/go-redis/redis/v8 v8.11.5
	github.com/go-redis/redis_rate/v9 v9.1.2
	github.com/godzie44/go-uring v0.0.0-20220926161041-69611e8b13d5
	github.com/gogo/protobuf v1.3.2
	github.com/golang/snappy v0.0.4
	github.com/grandcat/zeroconf v1.0.0
	github.com/hashicorp/go-multierror v1.1.1
	github.com/hashicorp/golang-lru v1.0.2
	github.com/jamiealquiza/tachymeter v2.0.0+incompatible
	github.com/juju/ratelimit v1.0.2
	github.com/julienschmidt/httprouter v1.3.0
	github.com/kavu/go_reuseport v1.5.0
	github.com/kr/pretty v0.3.1
	github.com/quic-go/quic-go v0.48.2
	github.com/rcrowley/go-metrics v0.0.0-20201227073835-cf1acfcdf475
	github.com/rpcxio/libkv v0.5.1
	github.com/rs/cors v1.11.1
	github.com/rubyist/circuitbreaker v2.2.1+incompatible
	github.com/smallnest/quick v0.2.0
	github.com/smallnest/statsview v1.0.1
	github.com/soheilhy/cmux v0.1.5
	github.com/stretchr/testify v1.11.1
	github.com/tinylib/msgp v1.2.5
	github.com/valyala/fastrand v1.1.0
	github.com/vmihailenco/msgpack/v5 v5.4.1
	github.com/xtaci/kcp-go v5.4.20+incompatible
	golang.org/x/exp v0.0.0-20250106191152-7588d65b2ba8 // indirect
	golang.org/x/net v0.50.0
	golang.org/x/sync v0.19.0
	google.golang.org/protobuf v1.36.11
)

require (
	github.com/cenk/backoff v2.2.1+incompatible // indirect
	github.com/cenkalti/backoff v2.2.1+incompatible // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dgryski/go-jump v0.0.0-20170409065014-e1f439676b57 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/facebookgo/clock v0.0.0-20150410010913-600d898af40a // indirect
	github.com/fsnotify/fsnotify v1.9.0 // indirect
	github.com/go-ole/go-ole v1.2.6 // indirect
	github.com/go-task/slim-sprig/v3 v3.0.0 // indirect
	github.com/google/pprof v0.0.0-20240430035430-e4905b036c4e // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/klauspost/cpuid/v2 v2.2.9 // indirect
	github.com/klauspost/reedsolomon v1.12.4 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/libp2p/go-sockaddr v0.1.1 // indirect
	github.com/lufia/plan9stats v0.0.0-20211012122336-39d0f177ccd0 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/miekg/dns v1.1.27 // indirect
	github.com/onsi/ginkgo/v2 v2.17.2 // indirect
	github.com/peterbourgon/g2s v0.0.0-20170223122336-d4e7ad98afea // indirect
	github.com/philhofer/fwd v1.1.3-0.20240916144458-20a13a1f6b7c // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/power-devops/perfstat v0.0.0-20210106213030-5aafc221ea8c // indirect
	github.com/rogpeppe/go-internal v1.10.0 // indirect
	github.com/shirou/gopsutil/v3 v3.23.12 // indirect
	github.com/shoenig/go-m1cpu v0.1.6 // indirect
	github.com/templexxx/cpufeat v0.0.0-20180724012125-cef66df7f161 // indirect
	github.com/templexxx/xor v0.0.0-20191217153810-f85b25db303b // indirect
	github.com/tjfoc/gmsm v1.4.1 // indirect
	github.com/tklauser/go-sysconf v0.3.12 // indirect
	github.com/tklauser/numcpus v0.6.1 // indirect
	github.com/vmihailenco/tagparser/v2 v2.0.0 // indirect
	github.com/xtaci/lossyconn v0.0.0-20200209145036-adba10fffc37 // indirect
	github.com/yusufpapurcu/wmi v1.2.3 // indirect
	go.uber.org/mock v0.4.0 // indirect
	golang.org/x/crypto v0.48.0 // indirect
	golang.org/x/mod v0.32.0 // indirect
	golang.org/x/sys v0.41.0 // indirect
	golang.org/x/text v0.34.0 // indirect
	golang.org/x/tools v0.41.0 // indirect
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
