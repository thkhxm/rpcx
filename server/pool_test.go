package server

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

type Elem struct {
	// magicNumber the Magic Number
	magicNumber int
}

func (e Elem) Reset() {

}

func TestPool(t *testing.T) {
	// 与生产路径（handleRequest 的 Get/Put）保持一致：池按值类型作 key，
	// 但池内流转的始终是指针（typePools.New 对非指针类型返回 reflect.New(t) 的指针）。
	// 旧测试 Put 值类型并断言 Get().(Elem)，在 -race 下 sync.Pool 会随机丢弃
	// Put 的对象并走 New 构造（返回 *Elem），类型断言直接 panic——属测试误用池。
	elemType := reflect.TypeOf(Elem{})
	// init Elem pool
	reflectTypePools.Init(elemType)
	reflectTypePools.Put(elemType, &Elem{magicNumber: 42})
	// Get() 可能返回刚 Put 的对象（magicNumber=42），也可能因 sync.Pool
	// 的丢弃语义走 New 新建零值对象（magicNumber=0），两者都合法
	got, ok := reflectTypePools.Get(elemType).(*Elem)
	if !ok {
		t.Fatalf("池中对象应为 *Elem")
	}
	assert.Contains(t, []int{0, 42}, got.magicNumber)
}
