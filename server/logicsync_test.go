package server

// 本文件覆盖 v3 F1 中 LogicSync 的工程质量修复：
//  1. WithLogicSyncPoolSize(<=0) 不再产生空锁池（旧实现请求期取模除零 panic）；
//  2. __hash 为空时不再退化为单锁全局串行，回退按连接散列；
//  3. 端到端验证"同一 __hash 串行、不同 __hash 并行"的核心语义。

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thkhxm/rpcx/v2/client"
	"github.com/thkhxm/rpcx/v2/share"
)

// TestWithLogicSyncPoolSize_NonPositive size<=0 必须被忽略并落回默认锁池，
// 不允许产生 len==0 的锁池（取模除零 panic 的雷）。
func TestWithLogicSyncPoolSize_NonPositive(t *testing.T) {
	for _, size := range []int{0, -1, -5000} {
		s := NewServer(WithLogicSync("A.B"), WithLogicSyncPoolSize(size))
		if len(s.logicLockPool) != defaultLogicSyncLockPoolSize {
			t.Fatalf("WithLogicSyncPoolSize(%d) 后锁池大小 %d，期望默认 %d",
				size, len(s.logicLockPool), defaultLogicSyncLockPoolSize)
		}
	}

	// 合法入参仍按指定大小生效
	s := NewServer(WithLogicSync("A.B"), WithLogicSyncPoolSize(7))
	if len(s.logicLockPool) != 7 {
		t.Fatalf("WithLogicSyncPoolSize(7) 后锁池大小 %d，期望 7", len(s.logicLockPool))
	}
	for i, l := range s.logicLockPool {
		if l == nil {
			t.Fatalf("锁池第 %d 个槽位是 nil", i)
		}
	}
}

// fakeAddr / fakeConn 是 logicSyncLockOf 单元测试用的最小 net.Conn 桩。
type fakeAddr string

func (a fakeAddr) Network() string { return "tcp" }
func (a fakeAddr) String() string  { return string(a) }

type fakeConn struct{ addr string }

func (c *fakeConn) Read(b []byte) (int, error)       { return 0, nil }
func (c *fakeConn) Write(b []byte) (int, error)      { return len(b), nil }
func (c *fakeConn) Close() error                     { return nil }
func (c *fakeConn) LocalAddr() net.Addr              { return fakeAddr("local") }
func (c *fakeConn) RemoteAddr() net.Addr             { return fakeAddr(c.addr) }
func (c *fakeConn) SetDeadline(time.Time) error      { return nil }
func (c *fakeConn) SetReadDeadline(time.Time) error  { return nil }
func (c *fakeConn) SetWriteDeadline(time.Time) error { return nil }

// ctxWithConn 构造带 RemoteConnContextKey 的 share.Context（模拟 serveConn 的产物）。
func ctxWithConn(addr string) *share.Context {
	return share.WithValue(context.Background(), RemoteConnContextKey, net.Conn(&fakeConn{addr: addr}))
}

// TestLogicSyncLockOf_EmptyHashPerConnFallback __hash 为空时按连接散列：
// 不同连接拿到不同锁（不再全局单锁串行），同一连接拿到同一把锁。
func TestLogicSyncLockOf_EmptyHashPerConnFallback(t *testing.T) {
	s := NewServer(WithLogicSync("X.Y"))
	poolSize := uint64(len(s.logicLockPool))

	addr1 := "10.0.0.1:1000"
	slot1 := share.HashString(addr1) % poolSize

	// 散列是确定性的，循环找一个落到不同槽位的地址必然终止
	addr2 := ""
	for i := 0; i < 1_000_000; i++ {
		cand := fmt.Sprintf("10.0.0.2:%d", 1000+i)
		if share.HashString(cand)%poolSize != slot1 {
			addr2 = cand
			break
		}
	}
	if addr2 == "" {
		t.Fatal("找不到散列到不同槽位的地址（不可能发生）")
	}

	l1 := s.logicSyncLockOf(ctxWithConn(addr1))
	l1Again := s.logicSyncLockOf(ctxWithConn(addr1))
	l2 := s.logicSyncLockOf(ctxWithConn(addr2))

	if l1 == nil || l2 == nil {
		t.Fatal("锁池已初始化时不应返回 nil")
	}
	if l1 != l1Again {
		t.Fatal("同一连接（空 hash）两次取锁不一致")
	}
	if l1 == l2 {
		t.Fatal("不同连接（空 hash）拿到同一把锁——空 hash 仍在退化为全局串行")
	}

	// 旧实现：所有空 hash 请求都命中 HashString("") 的槽位
	emptySlotLock := s.logicLockPool[share.HashString("")%poolSize]
	if l1 == emptySlotLock && l2 == emptySlotLock {
		t.Fatal("空 hash 请求仍然全部命中 HashString(\"\") 槽位")
	}
}

// TestLogicSyncLockOf_HashDominatesConn __hash 非空时锁只由 hash 决定，
// 与连接无关（跨连接的同一用户也要串行）。
func TestLogicSyncLockOf_HashDominatesConn(t *testing.T) {
	s := NewServer(WithLogicSync("X.Y"))

	mkCtx := func(addr, hash string) *share.Context {
		ctx := ctxWithConn(addr)
		ctx.SetReqMetaData(share.ContextKeyHash, hash)
		return ctx
	}

	lA1 := s.logicSyncLockOf(mkCtx("10.0.0.1:1000", "user-A"))
	lA2 := s.logicSyncLockOf(mkCtx("10.0.0.2:2000", "user-A"))
	if lA1 != lA2 {
		t.Fatal("同一 __hash 不同连接拿到不同锁，跨连接串行语义被破坏")
	}

	expected := s.logicLockPool[share.HashString("user-A")%uint64(len(s.logicLockPool))]
	if lA1 != expected {
		t.Fatal("__hash 非空时锁槽选择与 share.HashString(hash) 不一致")
	}
}

// TestLogicSyncLockOf_EmptyPoolNoPanic 锁池为空（未注册 LogicSync 方法等
// 异常组合）时返回 nil 跳过加锁，而不是除零 panic。
func TestLogicSyncLockOf_EmptyPoolNoPanic(t *testing.T) {
	s := &Server{}
	if l := s.logicSyncLockOf(share.NewContext(context.Background())); l != nil {
		t.Fatal("空锁池应返回 nil")
	}
}

// ---- 端到端用例 ----

type SyncArgs struct {
	N int
}

type SyncReply struct {
	N int
}

// slowSyncSvc 记录 Block 方法的当前/最大并发度。
type slowSyncSvc struct {
	cur atomic.Int32
	max atomic.Int32
}

func (s *slowSyncSvc) Block(ctx context.Context, args *SyncArgs, reply *SyncReply) error {
	c := s.cur.Add(1)
	for {
		m := s.max.Load()
		if c <= m || s.max.CompareAndSwap(m, c) {
			break
		}
	}
	time.Sleep(300 * time.Millisecond)
	s.cur.Add(-1)
	reply.N = args.N
	return nil
}

func waitAddr(t *testing.T, s *Server) string {
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

// TestLogicSync_SameHashSerial_DiffHashParallel 端到端验证 LogicSync 核心语义：
// 相同 __hash 的并发请求串行执行（最大并发 == 1），
// 不同 __hash（不同槽位）的并发请求并行执行（最大并发 >= 2）。
// 这同时是审计建议的"两个不同 uid 的 LogicSync 请求可并行"断言。
func TestLogicSync_SameHashSerial_DiffHashParallel(t *testing.T) {
	svc := &slowSyncSvc{}
	s := NewServer(WithLogicSync("SlowSync.Block"))
	_ = s.RegisterName("SlowSync", svc, "")
	go func() {
		_ = s.Serve("tcp", "127.0.0.1:0")
	}()
	defer s.Close()
	addr := waitAddr(t, s)

	c := client.NewClient(client.DefaultOption)
	if err := c.Connect("tcp", addr); err != nil {
		t.Fatalf("连接失败: %v", err)
	}
	defer c.Close()

	callWithHash := func(hash string, wg *sync.WaitGroup) {
		defer wg.Done()
		sc := share.NewContext(context.Background())
		sc.SetReqMetaData(share.ContextKeyHash, hash)
		reply := &SyncReply{}
		if err := c.Call(sc, "SlowSync", "Block", &SyncArgs{N: 1}, reply); err != nil {
			t.Errorf("调用失败: %v", err)
		}
	}

	// 阶段 1：相同 __hash 的 3 个并发请求必须串行（锁保证，非时序依赖）
	var wg sync.WaitGroup
	wg.Add(3)
	for i := 0; i < 3; i++ {
		go callWithHash("user-A", &wg)
	}
	wg.Wait()
	if got := svc.max.Load(); got != 1 {
		t.Fatalf("相同 __hash 的最大并发度 %d，期望 1（串行）", got)
	}

	// 阶段 2：不同 __hash（确保不同槽位）的 2 个并发请求可并行
	poolSize := uint64(len(s.logicLockPool))
	slotA := share.HashString("user-A") % poolSize
	hashB := ""
	for i := 0; i < 1_000_000; i++ {
		cand := fmt.Sprintf("user-B-%d", i)
		if share.HashString(cand)%poolSize != slotA {
			hashB = cand
			break
		}
	}
	if hashB == "" {
		t.Fatal("找不到散列到不同槽位的 hash（不可能发生）")
	}

	svc.max.Store(0)
	wg.Add(2)
	go callWithHash("user-A", &wg)
	go callWithHash(hashB, &wg)
	wg.Wait()
	if got := svc.max.Load(); got < 2 {
		t.Fatalf("不同 __hash 的最大并发度 %d，期望 ≥2（并行）——LogicSync 退化为全局串行", got)
	}
}

// TestLogicSync_PoolSizeZeroOption_E2E 旧实现下 WithLogicSyncPoolSize(0) +
// LogicSync 方法调用 = 请求期取模除零 panic（被 processOneRequest recover 吞掉，
// 客户端永远等不到响应）。修复后请求必须正常返回。
func TestLogicSync_PoolSizeZeroOption_E2E(t *testing.T) {
	svc := &slowSyncSvc{}
	s := NewServer(WithLogicSyncPoolSize(0), WithLogicSync("SlowSync.Block"))
	if len(s.logicLockPool) == 0 {
		t.Fatal("WithLogicSyncPoolSize(0) 产生了空锁池")
	}
	_ = s.RegisterName("SlowSync", svc, "")
	go func() {
		_ = s.Serve("tcp", "127.0.0.1:0")
	}()
	defer s.Close()
	addr := waitAddr(t, s)

	c := client.NewClient(client.DefaultOption)
	if err := c.Connect("tcp", addr); err != nil {
		t.Fatalf("连接失败: %v", err)
	}
	defer c.Close()

	// 不注入 __hash（空 hash 走按连接散列回退），带超时防卡死
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	reply := &SyncReply{}
	if err := c.Call(share.NewContext(ctx), "SlowSync", "Block", &SyncArgs{N: 9}, reply); err != nil {
		t.Fatalf("调用失败（旧实现此处除零 panic 导致无响应）: %v", err)
	}
	if reply.N != 9 {
		t.Fatalf("响应不符：期望 9 得到 %d", reply.N)
	}
}
