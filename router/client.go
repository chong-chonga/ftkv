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

type ServiceClient struct {
	rpcClients      []routerproto.RouterClient
	serverAddresses []string
	lastLeader      int
	timeout         time.Duration
	logEnabled      bool
}

const defaultTimeout = 5 * time.Second

func MakeServiceClient(routerAddresses []string) (*ServiceClient, error) {
	err := tool.Check(routerAddresses)
	if err != nil {
		return nil, err
	}
	serverCount := len(routerAddresses)
	rpcClients := make([]routerproto.RouterClient, serverCount)
	for i := 0; i < serverCount; i++ {
		conn, _ := grpc.Dial(routerAddresses[i], grpc.WithTransportCredentials(insecure.NewCredentials()))
		rpcClients[i] = routerproto.NewRouterClient(conn)
	}
	client := &ServiceClient{
		rpcClients:      rpcClients,
		serverAddresses: routerAddresses,
		lastLeader:      -1,
		timeout:         defaultTimeout,
		logEnabled:      true,
	}

	return client, nil
}

func (c *ServiceClient) logPrintf(format string, a ...interface{}) {
	if c.logEnabled {
		log.Printf(format, a...)
	}
}

func (c *ServiceClient) QueryLatest() (*routerproto.ClusterConfigWrapper, error) {
	return c.Query(routerproto.LatestConfigId)
}

func (c *ServiceClient) Query(configId int32) (*routerproto.ClusterConfigWrapper, error) {
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
	return ret.(*routerproto.ClusterConfigWrapper), nil
}

func (c *ServiceClient) callWithRetry(args interface{}, handleRPC func(args interface{}, rpcClient routerproto.RouterClient) (interface{}, bool, error)) (interface{}, error) {
	rpcClients := c.rpcClients
	serverCount := len(rpcClients)
	s := tool.ChooseServer(c.lastLeader, serverCount)
	rpcClient := rpcClients[s]
	for i := 0; i < serverCount; i++ {
		ret, ok, err := handleRPC(args, rpcClient)
		if err != nil {
			return nil, err
		}
		if ok {
			c.lastLeader = s
			return ret, nil
		}
		// else retry
		s++
		if s >= serverCount {
			s = 0
		}
		rpcClient = rpcClients[s]
	}
	return nil, errors.New("service unavailable")
}

func (c *ServiceClient) handleQuery(request interface{}, rpcClient routerproto.RouterClient) (interface{}, bool, error) {
	queryRequest := request.(*routerproto.QueryRequest)
	c.logPrintf("send query request:%v", queryRequest)

	var queryReply *routerproto.QueryReply
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
		if queryReply.ErrCode == routerproto.ErrCode_ERR_CODE_INVALID_ARGUMENT {
			return nil, false, errors.New(queryReply.ErrMessage)
		}
		if queryReply.ErrCode == routerproto.ErrCode_ERR_CODE_OK {
			return queryReply.ConfigWrapper, true, nil
		}
	}
	return nil, false, nil
}

type SystemClient struct {
	*ServiceClient
}

func MakeSystemClient(configData []byte) (*SystemClient, error) {
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
	rpcClients := make([]routerproto.RouterClient, serverCount)
	for i := 0; i < serverCount; i++ {
		conn, _ := grpc.Dial(serverAddresses[i], grpc.WithTransportCredentials(insecure.NewCredentials()))
		rpcClients[i] = routerproto.NewRouterClient(conn)
	}

	log.Printf("router client info: serverAddresses:%v, timeout=%dms, logEnabled=%v", serverAddresses, timeout, clientConfig.LogEnabled)
	client := &ServiceClient{
		rpcClients:      rpcClients,
		serverAddresses: serverAddresses,
		lastLeader:      -1,
		timeout:         time.Duration(timeout) * time.Millisecond,
		logEnabled:      clientConfig.LogEnabled,
	}

	c := &SystemClient{client}
	return c, nil
}

func (c *SystemClient) handleJoin(request interface{}, rpcClient routerproto.RouterClient) (interface{}, bool, error) {
	joinRequest := request.(*routerproto.JoinRequest)
	c.logPrintf("send join request:%v", joinRequest)
	var joinReply *routerproto.JoinReply
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
		if joinReply.ErrCode == routerproto.ErrCode_ERR_CODE_INVALID_ARGUMENT {
			return nil, false, errors.New(joinReply.ErrMessage)
		}
		if joinReply.ErrCode == routerproto.ErrCode_ERR_CODE_OK {
			return nil, true, nil
		}
	}
	return nil, false, nil
}

func (c *SystemClient) handleLeave(request interface{}, rpcClient routerproto.RouterClient) (interface{}, bool, error) {
	leaveRequest := request.(*routerproto.LeaveRequest)
	c.logPrintf("send leave request:%v", leaveRequest)
	var leaveReply *routerproto.LeaveReply
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
		if leaveReply.ErrCode == routerproto.ErrCode_ERR_CODE_INVALID_ARGUMENT {
			return nil, false, errors.New(leaveReply.ErrMessage)
		}
		if leaveReply.ErrCode == routerproto.ErrCode_ERR_CODE_OK {
			return nil, true, nil
		}
	}
	return nil, false, nil
}

func (c *SystemClient) Join(serverGroups map[int32][]string) error {
	if ok, _, errmsg := routerproto.ValidateReplicaGroups(serverGroups); !ok {
		return errors.New(errmsg)
	}
	if len(serverGroups) == 0 {
		return errors.New("empty server groups")
	}
	replicaGroups := make(map[int32]*routerproto.Servers)
	for gid, servers := range serverGroups {
		replicaGroups[gid] = &routerproto.Servers{Servers: servers}
	}
	joinRequest := &routerproto.JoinRequest{ReplicaGroups: replicaGroups}
	_, err := c.callWithRetry(joinRequest, c.handleJoin)
	if err != nil {
		return err
	}
	return nil
}

func (c *SystemClient) Leave(groupIds []int32) error {
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
