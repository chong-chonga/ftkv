package router

import (
	"context"
	"errors"
	"github.com/ftkv/v1/router/routerproto"
	"github.com/ftkv/v1/tool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"log"
	"time"
)

type ListenerClient struct {
	rpcClients      []routerproto.RouterClient
	serverAddresses []string
	lastLeader      int
	timeout         time.Duration
	logEnabled      bool
}

func MakeListenerClient(routerAddresses []string) (*ListenerClient, error) {
	err := tool.Check(routerAddresses)
	if err != nil {
		return nil, err
	}
	rpcClients := dialRouterClients(routerAddresses)
	client := &ListenerClient{
		rpcClients:      rpcClients,
		serverAddresses: routerAddresses,
		lastLeader:      -1,
		timeout:         defaultTimeout * time.Millisecond,
		logEnabled:      false,
	}

	return client, nil
}

func (c *ListenerClient) logPrintf(format string, a ...interface{}) {
	if c.logEnabled {
		log.Printf(format, a...)
	}
}

func (c *ListenerClient) Query(configId int32) (*ClusterConfig, error) {
	if ok, _, errmsg := routerproto.ValidateConfigId(configId); !ok {
		return nil, errors.New(errmsg)
	}
	request := &routerproto.QueryRequest{
		ConfigId: configId,
	}
	ret, err := c.callWithRetry(request, c.handleQuery)
	if err != nil {
		return nil, err
	}
	return ret.(*ClusterConfig), nil
}

func (c *ListenerClient) callWithRetry(args interface{}, handleRPC func(args interface{}, server int) (interface{}, bool, error)) (interface{}, error) {
	rpcClients := c.rpcClients
	serverCount := len(rpcClients)
	s := tool.ChooseServer(c.lastLeader, serverCount)
	for i := 0; i < serverCount; i++ {
		ret, ok, err := handleRPC(args, s)
		if ok {
			c.lastLeader = s
			return ret, nil
		}
		if err != nil {
			return nil, err
		}
		// else retry
		s++
		if s >= serverCount {
			s = 0
		}
	}
	return nil, errors.New("service unavailable")
}

func (c *ListenerClient) handleQuery(request interface{}, server int) (interface{}, bool, error) {
	rpcClient := c.rpcClients[server]
	queryRequest := request.(*routerproto.QueryRequest)
	c.logPrintf("send query request:%v to server:ip=%s", queryRequest, c.serverAddresses[server])
	var queryReply *routerproto.QueryReply
	var err error
	{
		ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
		defer cancel()
		queryReply, err = rpcClient.Query(ctx, queryRequest)
	}
	c.logPrintf("query result:%v, err=%v", queryReply, err)
	if err != nil {
		s, _ := status.FromError(err)
		if s.Code() == codes.DeadlineExceeded {
			return nil, false, errors.New("service unavailable: " + err.Error())
		}
	} else {
		if queryReply.ErrCode == routerproto.ErrCode_ERR_CODE_OK {
			configWrapper := queryReply.ConfigWrapper
			replicaGroups := make(map[int32][]string)
			for gid, servers := range configWrapper.ReplicaGroups {
				replicaGroups[gid] = servers.Servers
			}
			return &ClusterConfig{
				ConfigId:      configWrapper.ConfigId,
				ShardsMap:     configWrapper.ShardsMap,
				ReplicaGroups: replicaGroups,
			}, true, nil
		}
		if queryReply.ErrCode == routerproto.ErrCode_ERR_CODE_INVALID_ARGUMENT {
			return nil, false, errors.New(queryReply.ErrMessage)
		}
	}
	return nil, false, nil
}
