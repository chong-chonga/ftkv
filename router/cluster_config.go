package router

type ClusterConfig struct {
	ConfigId      int32
	ShardsMap     []int32
	ReplicaGroups map[int32][]string
}
