package client

// 本文件覆盖 v3 F1（fork 治理）的三项修复：
//  1. metadata map 并发竞态家族：client.Go/SendRaw 改为持锁拷贝
//     （配合 -race 运行，修复前必报 race / 线上为不可 recover 的 runtime fatal）；
//  2. client.input() 回退串行：同一连接上服务端推送必须有序到达
//     （ants 池并发版本会乱序 + err 变量竞态，与 LogicSync 目标矛盾）；
//  3. HashString 下沉 share 解 import 环：本文件能 import server 编译运行，
//     本身就是"client 包测试 → server → client 不再成环"的证明。

import (
	"context"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thkhxm/rpcx/v2/protocol"
	"github.com/thkhxm/rpcx/v2/server"
	"github.com/thkhxm/rpcx/v2/share"
)

// waitServerAddr 轮询等待 server 监听就绪并返回监听地址。
func waitServerAddr(t *testing.T, s *server.Server) string {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if addr := s.Address(); addr != nil {
			return addr.String()
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("server 启动超时")
	return ""
}

// TestHashString_CompatWithShare client.HashString 委托 share.HashString 后
// 散列值必须保持一致（一致性散列、LogicSync 锁槽都依赖该值稳定）。
func TestHashString_CompatWithShare(t *testing.T) {
	for _, s := range []string{"", "u1", "tcp@127.0.0.1:8972", "中文"} {
		if HashString(s) != share.HashString(s) {
			t.Fatalf("client.HashString(%q) 与 share.HashString 不一致", s)
		}
	}
}

// pushOrderSvc 把客户端连接对应的服务端 conn 捕获出来，供测试做服务端推送。
type pushOrderSvc struct {
	conns chan net.Conn
}

type PushArgs struct {
	A int
}

type PushReply struct {
	C int
}

func (p *pushOrderSvc) Hello(ctx context.Context, args *PushArgs, reply *PushReply) error {
	if conn, ok := ctx.Value(server.RemoteConnContextKey).(net.Conn); ok {
		select {
		case p.conns <- conn:
		default:
		}
	}
	reply.C = args.A
	return nil
}

// TestClientInput_ServerPushOrdering 同一连接上服务端推送的有序性测试：
// 服务端按 0..N-1 顺序推送 N 条消息，客户端必须按完全相同的顺序收到。
// input() 的 ants 池并发版本（commit 6f9e598）在此场景下可乱序投递；
// 回退串行后本测试必须确定性通过。
func TestClientInput_ServerPushOrdering(t *testing.T) {
	svc := &pushOrderSvc{conns: make(chan net.Conn, 1)}
	s := server.NewServer()
	_ = s.RegisterName("PushSvc", svc, "")
	go func() {
		_ = s.Serve("tcp", "127.0.0.1:0")
	}()
	defer s.Close()
	addr := waitServerAddr(t, s)

	const n = 500

	c := &Client{option: DefaultOption}
	ch := make(chan *protocol.Message, n)
	// 必须在 Connect（启动 input goroutine）之前注册推送通道
	c.RegisterServerMessageChan(ch)
	if err := c.Connect("tcp", addr); err != nil {
		t.Fatalf("连接失败: %v", err)
	}
	defer c.Close()

	// 先打一发 RPC，让服务端捕获本连接的 conn
	reply := &PushReply{}
	if err := c.Call(context.Background(), "PushSvc", "Hello", &PushArgs{A: 1}, reply); err != nil {
		t.Fatalf("调用失败: %v", err)
	}

	var serverConn net.Conn
	select {
	case serverConn = <-svc.conns:
	case <-time.After(3 * time.Second):
		t.Fatal("等待服务端捕获连接超时")
	}

	// 单 goroutine 顺序推送，TCP 保证到达顺序 == 发送顺序，
	// 因此客户端的接收顺序只取决于 input() 是否串行处理
	for i := 0; i < n; i++ {
		if err := s.SendMessage(serverConn, "push", "Order", nil, []byte(strconv.Itoa(i))); err != nil {
			t.Fatalf("第 %d 条推送发送失败: %v", i, err)
		}
	}

	for i := 0; i < n; i++ {
		select {
		case msg := <-ch:
			got, err := strconv.Atoi(string(msg.Payload))
			if err != nil {
				t.Fatalf("第 %d 条推送 payload 非法: %q", i, msg.Payload)
			}
			if got != i {
				t.Fatalf("推送乱序：第 %d 条收到 %d", i, got)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("等待第 %d 条推送超时（已收 %d 条）", i, i)
		}
	}
}

// TestClientCall_MetaConcurrentWrite_NoRace 复现 tgf 网关的真实并发模式：
// 每用户一个长生命周期 share.Context（tgf/rpc/tcp.go:534），
// 路由刷新/trace 注入 goroutine 持续 SetReqMetaData（tcp.go:1035/1149），
// 同时该用户的请求经 client.Go 拷贝同一张 meta map 发往后端。
// 修复前 client.Go 的裸 maps.Copy 在 -race 下报竞态，
// 线上表现为 'concurrent map iteration and map write' 整进程 fatal。
func TestClientCall_MetaConcurrentWrite_NoRace(t *testing.T) {
	s := server.NewServer()
	_ = s.RegisterName("Arith", new(Arith), "")
	go func() {
		_ = s.Serve("tcp", "127.0.0.1:0")
	}()
	defer s.Close()
	addr := waitServerAddr(t, s)

	c := &Client{option: DefaultOption}
	if err := c.Connect("tcp", addr); err != nil {
		t.Fatalf("连接失败: %v", err)
	}
	defer c.Close()

	const users = 4
	const callsPerUser = 200

	var wg sync.WaitGroup
	var failed atomic.Int32
	for u := 0; u < users; u++ {
		// 每个"用户"一个长生命周期 share.Context
		sc := share.NewContext(context.Background())
		sc.SetReqMetaData("UserId", "user-"+strconv.Itoa(u))

		stop := make(chan struct{})

		// 写方：模拟网关路由刷新 / 每请求 TRACEID 注入
		wg.Add(1)
		go func(sc *share.Context) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				sc.SetReqMetaData("TRACEID", strconv.Itoa(i))
				sc.SetReqMetaData("NodeId", strconv.Itoa(i%7))
			}
		}(sc)

		// 调用方：该用户的消息按连接语义顺序发出（与 tgf 网关一致）
		wg.Add(1)
		go func(sc *share.Context) {
			defer wg.Done()
			defer close(stop)
			args := &Args{A: 7, B: 6}
			for i := 0; i < callsPerUser; i++ {
				reply := &Reply{}
				if err := c.Call(sc, "Arith", "Mul", args, reply); err != nil {
					failed.Add(1)
					t.Errorf("调用失败: %v", err)
					return
				}
				if reply.C != 42 {
					failed.Add(1)
					t.Errorf("响应错乱：期望 42 得到 %d", reply.C)
					return
				}
			}
		}(sc)
	}
	wg.Wait()
	if failed.Load() > 0 {
		t.Fatalf("%d 次调用失败", failed.Load())
	}
}

// TestClientSendRaw_MetaConcurrentWrite_NoRace SendRaw 路径（网关原始转发链路）
// 的同族竞态：SendRaw 旧实现对 ctx meta map 做不持锁 range 拷贝。
func TestClientSendRaw_MetaConcurrentWrite_NoRace(t *testing.T) {
	s := server.NewServer()
	_ = s.RegisterName("Arith", new(Arith), "")
	go func() {
		_ = s.Serve("tcp", "127.0.0.1:0")
	}()
	defer s.Close()
	addr := waitServerAddr(t, s)

	c := &Client{option: DefaultOption}
	if err := c.Connect("tcp", addr); err != nil {
		t.Fatalf("连接失败: %v", err)
	}
	defer c.Close()

	sc := share.NewContext(context.Background())
	sc.SetReqMetaData("UserId", "raw-user")

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			sc.SetReqMetaData("TRACEID", strconv.Itoa(i))
		}
	}()

	codec := share.Codecs[protocol.MsgPack]
	for i := 0; i < 300; i++ {
		req := protocol.NewMessage()
		req.SetMessageType(protocol.Request)
		req.SetSeq(uint64(i + 1))
		req.ServicePath = "Arith"
		req.ServiceMethod = "Mul"
		req.SetSerializeType(protocol.MsgPack)
		payload, err := codec.Encode(&Args{A: 3, B: 5})
		if err != nil {
			t.Fatalf("编码失败: %v", err)
		}
		req.Payload = payload

		_, replyData, err := c.SendRaw(sc, req)
		if err != nil {
			t.Fatalf("SendRaw 失败: %v", err)
		}
		reply := &Reply{}
		if err = codec.Decode(replyData, reply); err != nil {
			t.Fatalf("解码失败: %v", err)
		}
		if reply.C != 15 {
			t.Fatalf("响应错乱：期望 15 得到 %d", reply.C)
		}
	}
	close(stop)
	wg.Wait()
}
