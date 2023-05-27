package router

import (
	"context"
	"errors"
	"github.com/ftkv/v1/router/routerproto"
	"github.com/ftkv/v1/tool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"gopkg.in/yaml.v3"
	"log"
	"time"
)

type ClientConfigWrapper struct {
	ClientConfig ClientConfig `yaml:"routerClient"`
}

type ClientConfig struct {
	ServerAddresses []string `yaml:"serverAddresses,flow"`
	Timeout         int      `yaml:"timeout"`
	LogEnabled      bool     `yaml:"logEnabled"`
}

func readClientConfig(configData []byte) (*ClientConfig, error) {
	if len(configData) == 0 {
		return nil, errors.New("configuration is empty")
	}
	conf := &ClientConfigWrapper{}
	err := yaml.Unmarshal(configData, conf)
	if err != nil {
		return nil, err
	}
	return &conf.ClientConfig, err
}

func dialRouterClients(routerAddresses []string) []routerproto.RouterClient {
	serverCount := len(routerAddresses)
	rpcClients := make([]routerproto.RouterClient, serverCount)
	for i := 0; i < serverCount; i++ {
		conn, _ := grpc.Dial(routerAddresses[i], grpc.WithTransportCredentials(insecure.NewCredentials()))
		rpcClients[i] = routerproto.NewRouterClient(conn)
	}
	return rpcClients
}

type Client struct {
	*ListenerClient
}

const defaultTimeout = 5000

func MakeClient(configData []byte) (*Client, error) {
	clientConfig, err := readClientConfig(configData)
	if err != nil {
		return nil, &tool.RuntimeError{Stage: "load config", Err: err}
	}
	serverAddresses := clientConfig.ServerAddresses
	err = tool.Check(serverAddresses)
	if err != nil {
		return nil, &tool.RuntimeError{Stage: "configure router client", Err: err}
	}
	timeout := clientConfig.Timeout
	if timeout < 0 {
		return nil, errors.New("timeout must be a positive number")
	} else if timeout == 0 {
		timeout = defaultTimeout
		log.Printf("configure router client info: using default timeout=%dms", defaultTimeout)
	}
	log.Printf("router client info: serverAddresses:%v, timeout=%dms, logEnabled=%v", serverAddresses, timeout, clientConfig.LogEnabled)

	rpcClients := dialRouterClients(serverAddresses)
	client := &ListenerClient{
		rpcClients:      rpcClients,
		serverAddresses: serverAddresses,
		lastLeader:      -1,
		timeout:         time.Duration(timeout) * time.Millisecond,
		logEnabled:      clientConfig.LogEnabled,
	}

	c := &Client{client}
	return c, nil
}

func (c *Client) handleJoin(request interface{}, server int) (interface{}, bool, error) {
	rpcClient := c.rpcClients[server]
	joinRequest := request.(*routerproto.JoinRequest)
	c.logPrintf("send join request:%v to server:ip=%s", joinRequest, c.serverAddresses[server])
	var joinReply *routerproto.JoinReply
	var err error
	{
		ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
		defer cancel()
		joinReply, err = rpcClient.Join(ctx, joinRequest)
	}
	c.logPrintf("join result:%v, err=%v", joinReply, err)
	if err != nil {
		s, _ := status.FromError(err)
		if s.Code() == codes.DeadlineExceeded {
			return nil, false, errors.New("service unavailable: " + err.Error())
		}
	} else {
		if joinReply.ErrCode == routerproto.ErrCode_ERR_CODE_OK {
			return nil, true, nil
		}
		if joinReply.ErrCode == routerproto.ErrCode_ERR_CODE_INVALID_ARGUMENT {
			return nil, false, errors.New(joinReply.ErrMessage)
		}
	}
	return nil, false, nil
}

func (c *Client) handleLeave(request interface{}, server int) (interface{}, bool, error) {
	rpcClient := c.rpcClients[server]
	leaveRequest := request.(*routerproto.LeaveRequest)
	c.logPrintf("send leave request:%v to server:ip=%s", leaveRequest, c.serverAddresses[server])
	var leaveReply *routerproto.LeaveReply
	var err error
	{
		ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
		defer cancel()
		leaveReply, err = rpcClient.Leave(ctx, leaveRequest)
	}
	c.logPrintf("leave result:%v, err=%v", leaveReply, err)
	if err != nil {
		s, _ := status.FromError(err)
		if s.Code() == codes.DeadlineExceeded {
			return nil, false, errors.New("service unavailable: " + err.Error())
		}
	} else {
		if leaveReply.ErrCode == routerproto.ErrCode_ERR_CODE_OK {
			return nil, true, nil
		}
		if leaveReply.ErrCode == routerproto.ErrCode_ERR_CODE_INVALID_ARGUMENT {
			return nil, false, errors.New(leaveReply.ErrMessage)
		}
	}
	return nil, false, nil
}

func (c *Client) Join(serverGroups map[int32][]string) error {
	if ok, _, errmsg := routerproto.ValidateReplicaGroups(serverGroups); !ok {
		return errors.New(errmsg)
	}
	replicaGroups := make(map[int32]*routerproto.Servers)
	for gid, servers := range serverGroups {
		replicaGroups[gid] = &routerproto.Servers{Servers: servers}
	}
	joinRequest := &routerproto.JoinRequest{ReplicaGroups: replicaGroups}
	_, err := c.callWithRetry(joinRequest, c.handleJoin)
	return err
}

func (c *Client) Leave(groupIds []int32) error {
	if ok, _, errmsg := routerproto.ValidateGroupIds(groupIds); !ok {
		return errors.New(errmsg)
	}
	leaveRequest := &routerproto.LeaveRequest{
		GroupIds: groupIds,
	}
	_, err := c.callWithRetry(leaveRequest, c.handleLeave)
	if err != nil {
		return err
	}
	return nil
}
