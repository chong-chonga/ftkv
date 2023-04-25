package router

import (
	"context"
	"errors"
	"github.com/kvservice/v1/router/protobuf"
	"github.com/kvservice/v1/tool"
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

type Client struct {
	rpcClients      []protobuf.RouterClient
	serverAddresses []string
	lastLeader      int
	timeout         time.Duration
	logEnabled      bool
}

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
	}
	serverCount := len(serverAddresses)
	rpcClients := make([]protobuf.RouterClient, serverCount)
	for i := 0; i < serverCount; i++ {
		conn, _ := grpc.Dial(serverAddresses[i], grpc.WithTransportCredentials(insecure.NewCredentials()))
		rpcClients[i] = protobuf.NewRouterClient(conn)
	}

	log.Printf("router client info: serverAddresses:%v, timeout=%dms, logEnabled=%v", serverAddresses, timeout, clientConfig.LogEnabled)
	client := &Client{
		rpcClients:      rpcClients,
		serverAddresses: serverAddresses,
		lastLeader:      -1,
		timeout:         time.Duration(timeout) * time.Millisecond,
		logEnabled:      clientConfig.LogEnabled,
	}

	return client, nil
}

func (c *Client) logPrintf(format string, a ...interface{}) {
	if c.logEnabled {
		log.Printf(format, a...)
	}
}

func (c *Client) handleQuery(request interface{}, rpcClient protobuf.RouterClient) (interface{}, bool, error) {
	queryRequest := request.(*protobuf.QueryRequest)
	c.logPrintf("send query request:%v", queryRequest)

	var queryReply *protobuf.QueryReply
	var err error
	{
		ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
		defer cancel()
		queryReply, err = rpcClient.Query(ctx, queryRequest)
	}
	c.logPrintf("receive query reply:%v, err=%v", queryReply, err)
	if err != nil {
		s, _ := status.FromError(err)
		if s.Code() == codes.DeadlineExceeded {
			return nil, false, errors.New("service unavailable")
		}
	} else {
		if queryReply.ErrCode == protobuf.ErrCode_ERR_CODE_INVALID_ARGUMENT {
			return nil, false, errors.New(queryReply.ErrMessage)
		}
		if queryReply.ErrCode == protobuf.ErrCode_ERR_CODE_OK {
			return queryReply.ConfigWrapper, true, nil
		}
	}
	return nil, false, nil
}

func (c *Client) handleJoin(request interface{}, rpcClient protobuf.RouterClient) (interface{}, bool, error) {
	joinRequest := request.(*protobuf.JoinRequest)
	c.logPrintf("send join request:%v", joinRequest)
	var joinReply *protobuf.JoinReply
	var err error
	{
		ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
		defer cancel()
		joinReply, err = rpcClient.Join(ctx, joinRequest)
	}
	c.logPrintf("receive join reply:%v, err=%v", joinReply, err)
	if err != nil {
		s, _ := status.FromError(err)
		if s.Code() == codes.DeadlineExceeded {
			return nil, false, errors.New("service unavailable")
		}
	} else {
		if joinReply.ErrCode == protobuf.ErrCode_ERR_CODE_INVALID_ARGUMENT {
			return nil, false, errors.New(joinReply.ErrMessage)
		}
		if joinReply.ErrCode == protobuf.ErrCode_ERR_CODE_OK {
			return nil, true, nil
		}
	}
	return nil, false, nil
}

func (c *Client) handleLeave(request interface{}, rpcClient protobuf.RouterClient) (interface{}, bool, error) {
	leaveRequest := request.(*protobuf.LeaveRequest)
	c.logPrintf("send leave request:%v", leaveRequest)
	var leaveReply *protobuf.LeaveReply
	var err error
	{
		ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
		defer cancel()
		leaveReply, err = rpcClient.Leave(ctx, leaveRequest)
	}
	c.logPrintf("receive leave reply:%v, err=%v", leaveReply, err)
	if err != nil {
		s, _ := status.FromError(err)
		if s.Code() == codes.DeadlineExceeded {
			return nil, false, errors.New("service unavailable")
		}
	} else {
		if leaveReply.ErrCode == protobuf.ErrCode_ERR_CODE_INVALID_ARGUMENT {
			return nil, false, errors.New(leaveReply.ErrMessage)
		}
		if leaveReply.ErrCode == protobuf.ErrCode_ERR_CODE_OK {
			return nil, true, nil
		}
	}
	return nil, false, nil
}

func (c *Client) callWithRetry(args interface{}, handleRPC func(args interface{}, rpcClient protobuf.RouterClient) (interface{}, bool, error)) (interface{}, error) {
	rpcClients := c.rpcClients
	serverCount := len(rpcClients)
	server := tool.ChooseServer(c.lastLeader, serverCount)
	rpcClient := rpcClients[server]
	for i := 0; i < serverCount; i++ {
		ret, ok, err := handleRPC(args, rpcClient)
		if err != nil {
			return nil, err
		}
		if ok {
			return ret, nil
		}
		// else retry
		server++
		if server >= serverCount {
			server = 0
		}
		rpcClient = rpcClients[server]
	}
	return nil, errors.New("service unavailable")
}

func (c *Client) Query(configId int32) (*protobuf.ClusterConfigWrapper, error) {
	if ok, _, errmsg := protobuf.ValidateConfigId(configId); !ok {
		return nil, errors.New(errmsg)
	}
	request := &protobuf.QueryRequest{
		ConfigId: configId,
	}
	ret, err := c.callWithRetry(request, c.handleQuery)
	if err != nil {
		return nil, err
	}
	return ret.(*protobuf.ClusterConfigWrapper), nil
}

func (c *Client) Join(serverGroups map[int32][]string) error {
	if ok, _, errmsg := protobuf.ValidateReplicaGroups(serverGroups); !ok {
		return errors.New(errmsg)
	}
	if len(serverGroups) == 0 {
		return errors.New("empty server groups")
	}
	replicaGroups := make(map[int32]*protobuf.Servers)
	for gid, servers := range serverGroups {
		replicaGroups[gid] = &protobuf.Servers{Servers: servers}
	}
	joinRequest := &protobuf.JoinRequest{ReplicaGroups: replicaGroups}
	_, err := c.callWithRetry(joinRequest, c.handleJoin)
	if err != nil {
		return err
	}
	return nil
}

func (c *Client) Leave(groupIds []int32) error {
	if ok, _, errmsg := protobuf.ValidateGroupIds(groupIds); !ok {
		return errors.New(errmsg)
	}
	leaveRequest := &protobuf.LeaveRequest{
		GroupIds: groupIds,
	}
	_, err := c.callWithRetry(leaveRequest, c.handleLeave)
	if err != nil {
		return err
	}
	return nil
}
