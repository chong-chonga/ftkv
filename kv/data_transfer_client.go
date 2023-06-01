package kv

import (
	"context"
	"errors"
	"github.com/ftkv/v1/kv/kvproto"
	"github.com/ftkv/v1/tool"
	"time"
)

type DataTransferClient struct {
	rpcClients      []kvproto.KVClient
	serverAddresses []string
	lastLeader      int
	timeout         time.Duration
}

func MakeDataTransferClient(serverAddresses []string) *DataTransferClient {
	rpcClients := dialKVClients(serverAddresses)
	client := &DataTransferClient{
		rpcClients:      rpcClients,
		serverAddresses: serverAddresses,
		lastLeader:      -1,
		timeout:         defaultTimeout * time.Millisecond,
	}
	return client
}

func (c *DataTransferClient) callTransfer(rpcClient kvproto.KVClient, req *kvproto.TransferRequest) (*kvproto.TransferReply, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()
	return rpcClient.Transfer(ctx, req)
}

func (c *DataTransferClient) Transfer(configId int32, shards []int32, data map[string]string) error {
	req := &kvproto.TransferRequest{
		ConfigId: configId,
		Shards:   shards,
		Data:     data,
	}
	rpcClients := c.rpcClients
	serverCount := len(rpcClients)
	s := tool.ChooseServer(c.lastLeader, serverCount)
	rpcClient := rpcClients[s]
	var reply *kvproto.TransferReply
	var err error
	for i := 0; i < serverCount; i++ {
		reply, err = c.callTransfer(rpcClient, req)
		if err == nil {
			if reply.ErrCode == kvproto.ResponseCode_OK {
				return nil
			}
			if reply.ErrCode == kvproto.ResponseCode_CONFIG_MISMATCH {
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
