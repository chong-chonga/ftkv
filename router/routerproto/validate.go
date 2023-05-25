package routerproto

import "strconv"

const LatestConfigId = -1

const EmptyGroupId = 0

func ValidateConfigId(configId int32) (bool, ErrCode, string) {
	if configId < 0 && configId != LatestConfigId {
		return false, ErrCode_ERR_CODE_INVALID_ARGUMENT, "config id is negative"
	}
	return true, ErrCode_ERR_CODE_OK, ""
}

func ValidateReplicaGroups(replicaGroups map[int32][]string) (bool, ErrCode, string) {
	if len(replicaGroups) == 0 {
		return false, ErrCode_ERR_CODE_INVALID_ARGUMENT, "join empty replica groups"
	}
	for groupId, servers := range replicaGroups {
		if groupId == EmptyGroupId {
			return false, ErrCode_ERR_CODE_INVALID_ARGUMENT, "join group with group id:" + strconv.Itoa(EmptyGroupId)
		}
		if len(servers) == 0 {
			return false, ErrCode_ERR_CODE_INVALID_ARGUMENT, "join empty servers with group id:" + strconv.Itoa(int(groupId))
		}
	}
	return true, ErrCode_ERR_CODE_OK, ""
}

func ValidateGroupIds(groupIds []int32) (bool, ErrCode, string) {
	if len(groupIds) == 0 {
		return false, ErrCode_ERR_CODE_INVALID_ARGUMENT, "leave empty groups"
	}
	//for _, groupId := range groupIds {
	//	if groupId == EmptyGroupId {
	//		return false, ErrCode_ERR_CODE_INVALID_ARGUMENT, "leave group with group id:" + strconv.Itoa(EmptyGroupId)
	//	}
	//}
	return true, ErrCode_ERR_CODE_OK, ""
}
