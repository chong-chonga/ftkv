package shardkv

import (
	"context"
	"errors"
	"github.com/ftkv/v1/shardkv/shardproto"
	"github.com/ftkv/v1/tool"
	"time"
)

type DataTransferClient struct {
	rpcClients      []shardproto.ShardKVClient
	serverAddresses []string
	lastLeader      int
	timeout         time.Duration
}

func MakeDataTransferClient(serverAddresses []string) *DataTransferClient {
	rpcClients := dialShardKVClients(serverAddresses)
	client := &DataTransferClient{
		rpcClients:      rpcClients,
		serverAddresses: serverAddresses,
		lastLeader:      -1,
		timeout:         defaultTimeout * time.Millisecond,
	}
	return client
}

func (c *DataTransferClient) callTransfer(rpcClient shardproto.ShardKVClient, req *shardproto.TransferRequest) (*shardproto.TransferReply, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()
	return rpcClient.Transfer(ctx, req)
}

func (c *DataTransferClient) Transfer(configId int32, shards []int32, data map[string]string) error {
	req := &shardproto.TransferRequest{
		ConfigId: configId,
		Shards:   shards,
		Data:     data,
	}
	rpcClients := c.rpcClients
	serverCount := len(rpcClients)
	s := tool.ChooseServer(c.lastLeader, serverCount)
	rpcClient := rpcClients[s]
	var reply *shardproto.TransferReply
	var err error
	for i := 0; i < serverCount; i++ {
		reply, err = c.callTransfer(rpcClient, req)
		if err == nil {
			if reply.ErrCode == shardproto.ResponseCode_OK {
				return nil
			}
			if reply.ErrCode == shardproto.ResponseCode_CONFIG_MISMATCH {
				return errors.New("config mismatch")
			}
		}
		// retry
		s++
		if s >= serverCount {
			s = 0
		}
		rpcClient = rpcClients[s]
	}
	return errors.New("server unavailable")
}
