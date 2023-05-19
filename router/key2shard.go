package router

import "hash/fnv"

const shardsCount = 1 << 4

const shardsCountMask = shardsCount - 1

func Key2Shard(key string) int {
	// 创建一个新的FNV哈希对象
	h := fnv.New32()

	// 将字符串添加到哈希对象中
	h.Write([]byte(key))

	// 获取哈希值并转换为int类型
	return int(h.Sum32()) & shardsCountMask
}
