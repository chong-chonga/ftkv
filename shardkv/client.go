package shardkv

import (
	"context"
	"errors"
	"github.com/ftkv/v1/router"
	"github.com/ftkv/v1/shardkv/shardproto"
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
	ClientConfig ClientConfig `yaml:"shardkvClient"`
}

type ClientConfig struct {
	RouterAddresses []string `yaml:"routerAddresses,flow"`
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

func dialShardKVClients(serverAddresses []string) []shardproto.ShardKVClient {
	serverCount := len(serverAddresses)
	rpcClients := make([]shardproto.ShardKVClient, serverCount)
	for i := 0; i < serverCount; i++ {
		conn, _ := grpc.Dial(serverAddresses[i], grpc.WithTransportCredentials(insecure.NewCredentials()))
		rpcClients[i] = shardproto.NewShardKVClient(conn)
	}
	return rpcClients
}

type Client struct {
	lastLeader    int
	timeout       time.Duration
	logEnabled    bool
	routerClient  *router.ProxyClient
	serverClients []shardproto.ShardKVClient
}

const defaultTimeout = 5000

func MakeClient(configData []byte) (*Client, error) {
	clientConfig, err := readClientConfig(configData)
	if err != nil {
		return nil, &tool.RuntimeError{Stage: "load config", Err: err}
	}
	routerAddresses := clientConfig.RouterAddresses
	routerClient, err := router.MakeProxyClient(routerAddresses)
	if err != nil {
		return nil, &tool.RuntimeError{Stage: "configure router client", Err: err}
	}
	timeout := clientConfig.Timeout
	if timeout < 0 {
		return nil, errors.New("timeout must be a positive number")
	} else if timeout == 0 {
		log.Printf("configure shardkv client info: using default timeout=%dms", defaultTimeout)
	}

	log.Printf("router client info: routerAddresses:%v, timeout=%dms, logEnabled=%v", routerAddresses, timeout,
		clientConfig.LogEnabled)
	client := &Client{
		routerClient: routerClient,
		lastLeader:   -1,
		timeout:      time.Duration(timeout) * time.Millisecond,
		logEnabled:   clientConfig.LogEnabled,
	}
	return client, nil
}

func (c *Client) logPrintf(format string, a ...interface{}) {
	if c.logEnabled {
		log.Printf(format, a...)
	}
}

func (c *Client) queryServersFor(shard int) ([]shardproto.ShardKVClient, error) {
	clusterConfig, err := c.routerClient.QueryLatest()
	if err != nil {
		return nil, err
	}
	groupId := clusterConfig.ShardsMap[shard]
	Servers := clusterConfig.ReplicaGroups[groupId]
	if Servers == nil || len(Servers) == 0 {
		return nil, errors.New("no server groups alive")
	}
	return dialShardKVClients(Servers), nil
}

func (c *Client) callWithRetry(shard int, args interface{}, handleRPC func(args interface{},
	rpcClient shardproto.ShardKVClient) (interface{}, bool, error, bool)) (interface{}, error) {
	rpcClients := c.serverClients
	var err error
	if len(rpcClients) == 0 {
		rpcClients, err = c.queryServersFor(shard)
		if err != nil {
			return nil, err
		}
		c.serverClients = rpcClients
	}
	serverCount := len(rpcClients)
	s := tool.ChooseServer(c.lastLeader, serverCount)
	rpcClient := rpcClients[s]
	retried := false
	for i := 0; i < serverCount; i++ {
		ret, ok, err, wrongGroup := handleRPC(args, rpcClient)
		if ok {
			c.lastLeader = s
			return ret, nil
		}
		if wrongGroup {
			rpcClients, err = c.queryServersFor(shard)
			if err != nil {
				return nil, err
			}
			if !retried {
				retried = true
				serverCount = len(rpcClients)
				i = -1
			}
			c.serverClients = rpcClients
			s = 0
		}
		if err != nil {
			return nil, err
		}
		// retry
		s++
		if s >= serverCount {
			s = 0
		}
		rpcClient = rpcClients[s]
	}
	return nil, errors.New("service unavailable")
}

func (c *Client) handleGet(request interface{}, rpcClient shardproto.ShardKVClient) (interface{}, bool, error, bool) {
	getRequest := request.(*shardproto.GetRequest)
	c.logPrintf("send get request:%v", getRequest)
	var reply *shardproto.GetReply
	var err error
	{
		ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
		defer cancel()
		reply, err = rpcClient.Get(ctx, getRequest)
	}
	c.logPrintf("receive get reply:%v, err=%v", reply, err)
	if err != nil {
		s, _ := status.FromError(err)
		if s.Code() == codes.DeadlineExceeded {
			return nil, false, errors.New("service unavailable"), false
		}
	} else {
		if reply.ErrCode == shardproto.ResponseCode_OK || reply.ErrCode == shardproto.ResponseCode_KEY_NOT_EXISTS {
			return &getReply{Value: reply.Value, Exist: reply.Exist}, true, nil, false
		}
		if reply.ErrCode == shardproto.ResponseCode_WRONG_GROUP {
			return nil, false, nil, true
		}
	}
	return nil, false, nil, false
}

func (c *Client) handleSet(request interface{}, rpcClient shardproto.ShardKVClient) (interface{}, bool, error, bool) {
	setRequest := request.(*shardproto.SetRequest)
	c.logPrintf("send set request:%v", setRequest)
	var setReply *shardproto.SetReply
	var err error
	{
		ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
		defer cancel()
		setReply, err = rpcClient.Set(ctx, setRequest)
	}
	c.logPrintf("receive set reply:%v, err=%v", setReply, err)
	if err != nil {
		s, _ := status.FromError(err)
		if s.Code() == codes.DeadlineExceeded {
			return nil, false, errors.New("service unavailable"), false
		}
	} else {
		if setReply.ErrCode == shardproto.ResponseCode_OK {
			return nil, true, nil, false
		}
		if setReply.ErrCode == shardproto.ResponseCode_WRONG_GROUP {
			return nil, false, nil, true
		}
	}
	return nil, false, nil, false
}

func (c *Client) handleDelete(request interface{}, rpcClient shardproto.ShardKVClient) (interface{}, bool, error,
	bool) {
	deleteRequest := request.(*shardproto.DeleteRequest)
	c.logPrintf("send delete request:%v", deleteRequest)
	var deleteReply *shardproto.DeleteReply
	var err error
	{
		ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
		defer cancel()
		deleteReply, err = rpcClient.Delete(ctx, deleteRequest)
	}
	c.logPrintf("receive delete reply:%v, err=%v", deleteReply, err)
	if err != nil {
		s, _ := status.FromError(err)
		if s.Code() == codes.DeadlineExceeded {
			return nil, false, errors.New("service unavailable"), false
		}
	} else {
		if deleteReply.ErrCode == shardproto.ResponseCode_OK {
			return nil, true, nil, false
		}
		if deleteReply.ErrCode == shardproto.ResponseCode_WRONG_GROUP {
			return nil, false, nil, true
		}
	}
	return nil, false, nil, false
}

type getReply struct {
	Value string
	Exist bool
}

func (c *Client) Get(key string) (string, bool, error) {
	shard := router.Key2Shard(key)
	req := &shardproto.GetRequest{Key: key}
	r, err := c.callWithRetry(shard, req, c.handleGet)
	if err != nil {
		return "", false, err
	}
	reply := r.(*getReply)
	return reply.Value, reply.Exist, nil
}

func (c *Client) Set(key string, val string) error {
	shard := router.Key2Shard(key)
	req := &shardproto.SetRequest{Key: key, Value: val}
	_, err := c.callWithRetry(shard, req, c.handleSet)
	return err
}

func (c *Client) Delete(key string) error {
	shard := router.Key2Shard(key)
	req := &shardproto.DeleteRequest{Key: key}
	_, err := c.callWithRetry(shard, req, c.handleDelete)
	return err
}
