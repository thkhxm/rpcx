package server

// 本文件覆盖 fork 的两处行为变更：
// 1. 安全默认值：HTTP1 API 网关与 JSON-RPC 2.0 入口默认关闭，
//    仅通过 WithHTTPGateway() / WithJSONRPC() 显式 opt-in 才开启
//    （上游默认开启且无鉴权，等于在每个服务端口暴露未授权调用面）。
// 2. pond v1.9 兼容：WithPool 通过 pondPoolAdapter 适配
//    pond v1.9 的 Stop() context.Context 新签名。

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"
	"time"
)

// 编译期断言：pond 适配器必须满足 WorkerPool 接口（pond v1.9 兼容的核心）。
var _ WorkerPool = pondPoolAdapter{}

// startGatewayTestServer 启动一个注册了 Arith 服务的测试 server，
// 返回监听地址与关闭函数。
func startGatewayTestServer(t *testing.T, serviceName string, opts ...OptionFn) (addr string, shutdown func()) {
	t.Helper()
	s := NewServer(opts...)
	if err := s.RegisterName(serviceName, new(Arith), ""); err != nil {
		t.Fatalf("注册服务失败: %v", err)
	}
	go s.Serve("tcp", "127.0.0.1:0")

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if a := s.Address(); a != nil {
			addr = a.String()
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if addr == "" {
		t.Fatal("server 未在 3s 内完成监听")
	}
	shutdown = func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = s.Shutdown(ctx)
	}
	return addr, shutdown
}

// httpInvoke 通过 HTTP1 API 网关协议调用一次 RPC，返回 http 响应与错误。
func httpInvoke(addr, servicePath, serviceMethod string, payload []byte) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodPost, "http://"+addr+"/", bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set(XMessageID, "1")
	req.Header.Set(XServicePath, servicePath)
	req.Header.Set(XServiceMethod, serviceMethod)
	req.Header.Set(XSerializeType, "1") // protocol.JSON
	client := &http.Client{Timeout: 2 * time.Second}
	return client.Do(req)
}

// jsonrpcInvoke 通过 JSON-RPC 2.0 入口调用一次 RPC。
func jsonrpcInvoke(addr string, body string) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodPost, "http://"+addr+"/", bytes.NewReader([]byte(body)))
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-JSONRPC-2.0", "true")
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: 2 * time.Second}
	return client.Do(req)
}

// TestNewServerSecureGatewayDefaults 验证安全默认值：
// HTTP1 网关与 JSON-RPC 默认关闭，且 opt-in option 能显式打开。
func TestNewServerSecureGatewayDefaults(t *testing.T) {
	tests := []struct {
		name                  string
		opts                  []OptionFn
		wantHTTPGatewayOff    bool
		wantJSONRPCGatewayOff bool
	}{
		{"默认全关", nil, true, true},
		{"显式开启HTTP网关", []OptionFn{WithHTTPGateway()}, false, true},
		{"显式开启JSONRPC", []OptionFn{WithJSONRPC()}, true, false},
		{"两者都开启", []OptionFn{WithHTTPGateway(), WithJSONRPC()}, false, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewServer(tt.opts...)
			if s.DisableHTTPGateway != tt.wantHTTPGatewayOff {
				t.Errorf("DisableHTTPGateway = %v, 期望 %v", s.DisableHTTPGateway, tt.wantHTTPGatewayOff)
			}
			if s.DisableJSONRPC != tt.wantJSONRPCGatewayOff {
				t.Errorf("DisableJSONRPC = %v, 期望 %v", s.DisableJSONRPC, tt.wantJSONRPCGatewayOff)
			}
		})
	}
}

// TestHTTP1GatewayDefaultOffAndOptIn 行为级验证：
// 默认配置下 HTTP 调用 RPC 必须失败；WithHTTPGateway() 后同样的请求成功。
func TestHTTP1GatewayDefaultOffAndOptIn(t *testing.T) {
	payload, _ := json.Marshal(&Args{A: 10, B: 20})

	t.Run("默认关闭_HTTP调用被拒", func(t *testing.T) {
		addr, shutdown := startGatewayTestServer(t, "GwArithOff")
		defer shutdown()

		resp, err := httpInvoke(addr, "GwArithOff", "Mul", payload)
		if err == nil {
			defer resp.Body.Close()
			// 没有任何 HTTP matcher 时连接应被 cmux 直接关闭；
			// 万一拿到响应，也绝不允许是一次成功的 RPC 调用。
			if resp.StatusCode == http.StatusOK && resp.Header.Get(XMessageStatusType) != "Error" {
				t.Fatal("默认配置下 HTTP1 网关仍可调用 RPC，安全默认值失效")
			}
		}
	})

	t.Run("显式开启_HTTP调用成功", func(t *testing.T) {
		addr, shutdown := startGatewayTestServer(t, "GwArithOn", WithHTTPGateway())
		defer shutdown()

		resp, err := httpInvoke(addr, "GwArithOn", "Mul", payload)
		if err != nil {
			t.Fatalf("opt-in 后 HTTP 调用失败: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("opt-in 后期望 200，实际 %d", resp.StatusCode)
		}
		if errMsg := resp.Header.Get(XErrorMessage); errMsg != "" {
			t.Fatalf("opt-in 后 RPC 返回错误: %s", errMsg)
		}
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("读取响应失败: %v", err)
		}
		var reply Reply
		if err = json.Unmarshal(body, &reply); err != nil {
			t.Fatalf("解码响应失败: %v, body=%q", err, body)
		}
		if reply.C != 200 {
			t.Fatalf("期望 C=200，实际 %d", reply.C)
		}
	})
}

// TestJSONRPC2DefaultOffAndOptIn 行为级验证：
// 默认配置下 JSON-RPC 调用必须失败；WithJSONRPC() 后同样的请求成功。
func TestJSONRPC2DefaultOffAndOptIn(t *testing.T) {
	const reqBody = `{"jsonrpc":"2.0","method":"GwArithJSON.Mul","params":{"A":7,"B":6},"id":1}`

	t.Run("默认关闭_JSONRPC调用被拒", func(t *testing.T) {
		addr, shutdown := startGatewayTestServer(t, "GwArithJSON")
		defer shutdown()

		resp, err := jsonrpcInvoke(addr, reqBody)
		if err == nil {
			defer resp.Body.Close()
			body, _ := io.ReadAll(resp.Body)
			var res jsonrpcRespone
			if json.Unmarshal(body, &res) == nil && res.Result != nil {
				t.Fatal("默认配置下 JSON-RPC 入口仍可调用 RPC，安全默认值失效")
			}
		}
	})

	t.Run("显式开启_JSONRPC调用成功", func(t *testing.T) {
		addr, shutdown := startGatewayTestServer(t, "GwArithJSON", WithJSONRPC())
		defer shutdown()

		resp, err := jsonrpcInvoke(addr, reqBody)
		if err != nil {
			t.Fatalf("opt-in 后 JSON-RPC 调用失败: %v", err)
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("读取响应失败: %v", err)
		}
		var res jsonrpcRespone
		if err = json.Unmarshal(body, &res); err != nil {
			t.Fatalf("解码 JSON-RPC 响应失败: %v, body=%q", err, body)
		}
		if res.Error != nil {
			t.Fatalf("opt-in 后 JSON-RPC 返回错误: %+v", res.Error)
		}
		if res.Result == nil {
			t.Fatalf("opt-in 后 JSON-RPC 无结果, body=%q", body)
		}
		var reply Reply
		if err = json.Unmarshal(*res.Result, &reply); err != nil {
			t.Fatalf("解码 result 失败: %v", err)
		}
		if reply.C != 42 {
			t.Fatalf("期望 C=42，实际 %d", reply.C)
		}
	})
}

// TestWithPoolPondV19Adapter 验证 pond v1.9 适配器：
// Submit 真实执行任务，Stop / StopAndWait / StopAndWaitFor 三个停止入口均可正常调用。
func TestWithPoolPondV19Adapter(t *testing.T) {
	tests := []struct {
		name string
		stop func(p WorkerPool)
	}{
		{"Stop", func(p WorkerPool) { p.Stop() }},
		{"StopAndWait", func(p WorkerPool) { p.StopAndWait() }},
		{"StopAndWaitFor", func(p WorkerPool) { p.StopAndWaitFor(time.Second) }},
	}
	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewServer(WithPool(2, 8))
			if s.pool == nil {
				t.Fatal("WithPool 未设置 pool")
			}
			done := make(chan int, 1)
			n := i
			s.pool.Submit(func() { done <- n })
			select {
			case got := <-done:
				if got != n {
					t.Fatalf("任务结果错乱: 期望 %d 实际 %d", n, got)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("Submit 的任务 2s 内未执行")
			}
			tt.stop(s.pool) // 不应 panic
		})
	}
}
