package share

import (
	"context"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"sync"
)

// var _ context.Context = &Context{}

// Context is a rpcx customized Context that can contains multiple values.
type Context struct {
	tagsLock *sync.Mutex
	tags     map[interface{}]interface{}
	context.Context
}

func NewContext(ctx context.Context) *Context {
	tagsLock := &sync.Mutex{}
	ctx = context.WithValue(ctx, ContextTagsLock, tagsLock)
	return &Context{
		tagsLock: tagsLock,
		Context:  ctx,
		tags:     map[interface{}]interface{}{isShareContext: true},
	}
}

func (c *Context) Lock() {
	c.tagsLock.Lock()
}

func (c *Context) Unlock() {
	c.tagsLock.Unlock()
}

func (c *Context) Value(key interface{}) interface{} {
	c.tagsLock.Lock()
	defer c.tagsLock.Unlock()
	if c.tags == nil {
		c.tags = make(map[interface{}]interface{})
	}

	if v, ok := c.tags[key]; ok {
		return v
	}
	return c.Context.Value(key)
}

func (c *Context) SetValue(key, val interface{}) {
	c.tagsLock.Lock()
	defer c.tagsLock.Unlock()

	if c.tags == nil {
		c.tags = make(map[interface{}]interface{})
	}
	c.tags[key] = val
}

// DeleteKey delete the kv pair by key.
func (c *Context) DeleteKey(key interface{}) {
	c.tagsLock.Lock()
	defer c.tagsLock.Unlock()

	if c.tags == nil || key == nil {
		return
	}
	delete(c.tags, key)
}

func (c *Context) GetReqMetaDataByKey(key string) string {
	c.tagsLock.Lock()
	defer c.tagsLock.Unlock()
	meta := c.getReqMetaData()
	if meta == nil {
		return ""
	}
	return meta[key]
}
func (c *Context) getReqMetaData() map[string]string {
	var meta map[string]string
	if c.tags == nil {
		c.tags = make(map[interface{}]interface{})
	}

	if v, ok := c.tags[ReqMetaDataKey]; ok {
		meta = v.(map[string]string)
	} else if va, ok2 := c.Context.Value(ReqMetaDataKey).(map[string]string); ok2 {
		meta = va
	}
	return meta
}

func (c *Context) GetAllReqMetaDataKeys() (keys []string) {
	c.tagsLock.Lock()
	defer c.tagsLock.Unlock()
	tmpMaps := c.getReqMetaData()
	keys = slices.Collect(maps.Keys(tmpMaps))
	return
}

// CopyReqMetaData 在持有 tagsLock 的情况下对请求 metadata 做一次深拷贝快照。
//
// 背景：网关侧存在长生命周期的 share.Context（每用户一个），其 metadata map
// 会被其他 goroutine 通过 SetReqMetaData 并发写入；client 发请求前如果对
// 同一 map 做不持锁的迭代拷贝（旧实现 client.Go 里的 maps.Copy），会触发
// Go runtime 级 'concurrent map iteration and map write' fatal——不可 recover，
// 整个进程直接崩溃。所有"读取整个 meta map"的场景都必须走本方法。
// 无 metadata 时返回 nil。
func (c *Context) CopyReqMetaData() map[string]string {
	c.tagsLock.Lock()
	defer c.tagsLock.Unlock()
	meta := c.getReqMetaData()
	if meta == nil {
		return nil
	}
	cp := make(map[string]string, len(meta))
	maps.Copy(cp, meta)
	return cp
}

// CopyReqMetaDataFromContext 是 CopyReqMetaData 的 context.Context 通用版。
//
// ctx 可能不是 *share.Context 本体，而是它被 context.WithCancel/WithTimeout
// 等包装后的派生 context（例如 xclient 的 Failbackup 路径）。这种情况下类型
// 断言拿不到 *share.Context，但 NewContext 已把 tagsLock 以 ContextTagsLock
// 为键挂进了 context 链，这里通过 Value 把锁找回来再拷贝，保证同一把锁
// 保护同一张 map。完全没有锁可寻时退化为普通拷贝（纯 context 场景，
// map 由调用方独占，维持上游原语义）。
func CopyReqMetaDataFromContext(ctx context.Context) map[string]string {
	if sc, ok := ctx.(*Context); ok {
		return sc.CopyReqMetaData()
	}
	meta, _ := ctx.Value(ReqMetaDataKey).(map[string]string)
	if meta == nil {
		return nil
	}
	if lk, _ := ctx.Value(ContextTagsLock).(*sync.Mutex); lk != nil {
		lk.Lock()
		defer lk.Unlock()
	}
	cp := make(map[string]string, len(meta))
	maps.Copy(cp, meta)
	return cp
}

func (c *Context) SetReqMetaData(key, val string) {
	c.tagsLock.Lock()
	defer c.tagsLock.Unlock()
	meta := c.getReqMetaData()
	if meta == nil {
		meta = make(map[string]string)
		c.tags[ReqMetaDataKey] = meta
	}
	meta[key] = val
}

func (c *Context) String() string {
	return fmt.Sprintf("%v.WithValue(%v)", c.Context, c.tags)
}

func WithValue(parent context.Context, key, val interface{}) *Context {
	if key == nil {
		panic("nil key")
	}
	if !reflect.TypeOf(key).Comparable() {
		panic("key is not comparable")
	}

	tags := make(map[interface{}]interface{})
	tags[key] = val
	return &Context{Context: parent, tags: tags, tagsLock: &sync.Mutex{}}
}

func WithLocalValue(ctx *Context, key, val interface{}) *Context {
	if key == nil {
		panic("nil key")
	}
	if !reflect.TypeOf(key).Comparable() {
		panic("key is not comparable")
	}

	if ctx.tags == nil {
		ctx.tags = make(map[interface{}]interface{})
	}

	ctx.tags[key] = val
	return ctx
}

// IsShareContext checks whether a context is share.Context.
func IsShareContext(ctx context.Context) bool {
	ok := ctx.Value(isShareContext)
	return ok != nil
}
