package share

import "hash/fnv"

// HashString 计算字符串的 FNV-1a 64 位散列值。
//
// 实现原先位于 client 包（client/hash_utils.go）。server 包的 LogicSync
// 锁槽选择也需要它，但 server 反向 import client 会与 client 包的
// in-package 测试（package client → import server → import client）构成
// import 环，导致 go vet/go test ./client 无法编译。为解环把实现下沉到
// 双方共同依赖的 share 包；client.HashString 保留为兼容别名委托到这里。
func HashString(s string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(s))
	return h.Sum64()
}
