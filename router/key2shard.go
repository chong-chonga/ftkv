package router

import "hash/fnv"

const shardsCount = 1 << 4

const shardsCountMask = shardsCount - 1

func Key2Shard(key string) int {
	h := fnv.New32()
	h.Write([]byte(key))
	return int(h.Sum32()) & shardsCountMask
}
