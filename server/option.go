package server

import (
	"crypto/tls"
	"sync"
	"time"

	"github.com/alitto/pond"
)

// OptionFn configures options of server.
type OptionFn func(*Server)

// // WithOptions sets multiple options.
// func WithOptions(ops map[string]interface{}) OptionFn {
// 	return func(s *Server) {
// 		for k, v := range ops {
// 			s.options[k] = v
// 		}
// 	}
// }

// WithTLSConfig sets tls.Config.
func WithTLSConfig(cfg *tls.Config) OptionFn {
	return func(s *Server) {
		s.tlsConfig = cfg
	}
}

// WithReadTimeout sets readTimeout.
func WithReadTimeout(readTimeout time.Duration) OptionFn {
	return func(s *Server) {
		s.readTimeout = readTimeout
	}
}

// WithWriteTimeout sets writeTimeout.
func WithWriteTimeout(writeTimeout time.Duration) OptionFn {
	return func(s *Server) {
		s.writeTimeout = writeTimeout
	}
}

// WithLogicSync  add logic sync method, example : WithLogicSync("ServiceName.MethodName")
func WithLogicSync(serviceMethod string) OptionFn {
	return func(s *Server) {
		s.logicSyncMethod[serviceMethod] = true
	}
}

// WithLogicSyncPoolSize sets logic sync pool size.
func WithLogicSyncPoolSize(size int) OptionFn {
	return func(s *Server) {
		s.logicLockPool = make([]*sync.Mutex, size)
		for i := 0; i < size; i++ {
			s.logicLockPool[i] = &sync.Mutex{}
		}
	}
}

// pondPoolAdapter 把 pond v1.9 的 *pond.WorkerPool 适配成 rpcx 的 WorkerPool 接口。
// pond 自 v1.9.0 起把 Stop() 的签名从 `Stop()` 改成 `Stop() context.Context`，
// 导致 *pond.WorkerPool 不再直接实现 WorkerPool 接口（编译失败）。
// 这里包一层、丢弃返回值，使 fork 同时兼容 pond v1.9.x，
// 不再依赖 go.mod 里只在主模块生效、对下游不传播的 replace 钉旧版方案。
type pondPoolAdapter struct {
	*pond.WorkerPool
}

// Stop 停止池并丢弃 pond v1.9 新增的 context.Context 返回值。
func (p pondPoolAdapter) Stop() {
	_ = p.WorkerPool.Stop()
}

// WithPool sets goroutine pool.
func WithPool(maxWorkers, maxCapacity int, options ...pond.Option) OptionFn {
	return func(s *Server) {
		s.pool = pondPoolAdapter{pond.New(maxWorkers, maxCapacity, options...)}
	}
}

// WithCustomPool uses a custom goroutine pool.
func WithCustomPool(pool WorkerPool) OptionFn {
	return func(s *Server) {
		s.pool = pool
	}
}

// WithAsyncWrite sets AsyncWrite to true.
func WithAsyncWrite() OptionFn {
	return func(s *Server) {
		s.AsyncWrite = true
	}
}

// WithHTTPGateway 显式开启 HTTP1 API 网关（安全考虑默认关闭）。
// 开启后，任何能连到服务端口的客户端都可以通过带 X-RPCX-* 头的普通
// HTTP POST/GET/PUT 调用任意已注册的 RPC 方法；除非同时配置了
// Server.AuthFunc 做鉴权，否则不要在不可信网络上开启。
func WithHTTPGateway() OptionFn {
	return func(s *Server) {
		s.DisableHTTPGateway = false
	}
}

// WithJSONRPC 显式开启 JSON-RPC 2.0 入口（安全考虑默认关闭）。
// 开启后，带 X-JSONRPC-2.0:true 头的 HTTP 请求可以直接调用任意已注册
// 的 RPC 方法；与 WithHTTPGateway 同理，公网端口务必配合 AuthFunc 使用。
func WithJSONRPC() OptionFn {
	return func(s *Server) {
		s.DisableJSONRPC = false
	}
}
