package share

import (
	"context"
	"hash/fnv"
	"strconv"
	"sync"
	"testing"
	"time"
)

// TestCopyReqMetaData_Basic 验证持锁拷贝的基本语义：
// 无 meta 返回 nil；有 meta 返回深拷贝；改拷贝不影响原 map。
func TestCopyReqMetaData_Basic(t *testing.T) {
	ctx := NewContext(context.Background())
	if got := ctx.CopyReqMetaData(); got != nil {
		t.Fatalf("无 metadata 时应返回 nil，实际 %v", got)
	}

	ctx.SetReqMetaData("UserId", "u1")
	ctx.SetReqMetaData("TRACEID", "t1")

	cp := ctx.CopyReqMetaData()
	if cp["UserId"] != "u1" || cp["TRACEID"] != "t1" {
		t.Fatalf("拷贝内容不符：%v", cp)
	}

	// 改拷贝不影响原 map
	cp["UserId"] = "hacked"
	if ctx.GetReqMetaDataByKey("UserId") != "u1" {
		t.Fatalf("修改拷贝影响了原 metadata，不是深拷贝")
	}
}

// TestCopyReqMetaData_ConcurrentWithSet 复现 tgf 网关的并发模式：
// 长生命周期 share.Context 被一个 goroutine 持续 SetReqMetaData（写），
// 另一组 goroutine 持续 CopyReqMetaData（迭代拷贝）。
// 修复前（client.Go 裸 maps.Copy）该模式在 -race 下报竞态、
// 线上触发 'concurrent map iteration and map write' fatal；
// 修复后本测试在 -race 下必须干净通过。
func TestCopyReqMetaData_ConcurrentWithSet(t *testing.T) {
	ctx := NewContext(context.Background())
	ctx.SetReqMetaData("UserId", "u1")

	const writers = 2
	const readers = 4
	const iterations = 5000

	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				ctx.SetReqMetaData("TRACEID", strconv.Itoa(w*iterations+i))
				ctx.SetReqMetaData("Node"+strconv.Itoa(i%17), strconv.Itoa(i))
			}
		}(w)
	}
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				cp := ctx.CopyReqMetaData()
				if cp["UserId"] != "u1" {
					t.Errorf("拷贝丢失了稳定键 UserId：%v", cp)
					return
				}
			}
		}()
	}
	wg.Wait()
}

// TestCopyReqMetaDataFromContext_WrappedContext 验证 share.Context 被
// context.WithCancel/WithTimeout 包装后（xclient Failbackup 等路径），
// 仍能通过链上的 ContextTagsLock 找回同一把锁完成持锁拷贝。
func TestCopyReqMetaDataFromContext_WrappedContext(t *testing.T) {
	sc := NewContext(context.Background())
	sc.SetReqMetaData("UserId", "u1")

	wrapped, cancel := context.WithCancel(sc)
	defer cancel()
	wrapped2, cancel2 := context.WithTimeout(wrapped, time.Minute)
	defer cancel2()

	// 包装后类型断言已拿不到 *share.Context
	if _, ok := wrapped2.(*Context); ok {
		t.Fatal("测试前提不成立：包装后不应是 *share.Context")
	}

	// 锁必须可经 context 链发现（NewContext 挂进链的 ContextTagsLock）
	if lk, _ := wrapped2.Value(ContextTagsLock).(*sync.Mutex); lk == nil {
		t.Fatal("包装后的 context 链上找不到 ContextTagsLock，持锁拷贝失效")
	}

	cp := CopyReqMetaDataFromContext(wrapped2)
	if cp["UserId"] != "u1" {
		t.Fatalf("经包装 context 拷贝 metadata 失败：%v", cp)
	}

	// 包装 context 上的并发写 + 拷贝在 -race 下必须干净
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 5000; i++ {
			sc.SetReqMetaData("TRACEID", strconv.Itoa(i))
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 5000; i++ {
			_ = CopyReqMetaDataFromContext(wrapped2)
		}
	}()
	wg.Wait()
}

// TestCopyReqMetaDataFromContext_PlainContext 纯 context（无 share.Context
// 参与）维持上游原语义：直接拷贝调用方独占的 map。
func TestCopyReqMetaDataFromContext_PlainContext(t *testing.T) {
	if got := CopyReqMetaDataFromContext(context.Background()); got != nil {
		t.Fatalf("无 metadata 的纯 context 应返回 nil，实际 %v", got)
	}

	ctx := context.WithValue(context.Background(), ReqMetaDataKey, map[string]string{"k": "v"})
	cp := CopyReqMetaDataFromContext(ctx)
	if cp["k"] != "v" {
		t.Fatalf("纯 context 拷贝失败：%v", cp)
	}
}

// TestGetAllReqMetaDataKeys_StdlibMaps 迁移到标准库 maps/slices 后行为不变。
func TestGetAllReqMetaDataKeys_StdlibMaps(t *testing.T) {
	ctx := NewContext(context.Background())
	if keys := ctx.GetAllReqMetaDataKeys(); len(keys) != 0 {
		t.Fatalf("无 metadata 时应返回空 keys，实际 %v", keys)
	}

	ctx.SetReqMetaData("a", "1")
	ctx.SetReqMetaData("b", "2")
	keys := ctx.GetAllReqMetaDataKeys()
	if len(keys) != 2 {
		t.Fatalf("期望 2 个 key，实际 %v", keys)
	}
	seen := map[string]bool{}
	for _, k := range keys {
		seen[k] = true
	}
	if !seen["a"] || !seen["b"] {
		t.Fatalf("keys 内容不符：%v", keys)
	}
}

// TestHashString_Fnv64a HashString 下沉到 share 后散列算法保持 FNV-1a 64 不变
// （server LogicSync 锁槽选择与 client 一致性散列都依赖该值的稳定性）。
func TestHashString_Fnv64a(t *testing.T) {
	for _, s := range []string{"", "u1", "10.0.0.1:8080", "中文键"} {
		h := fnv.New64a()
		h.Write([]byte(s))
		if want, got := h.Sum64(), HashString(s); want != got {
			t.Fatalf("HashString(%q) = %d，期望 fnv-1a 64 的 %d", s, got, want)
		}
	}
}
