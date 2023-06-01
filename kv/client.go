package kv

import (
	"context"
	"errors"
	"github.com/ftkv/v1/kv/kvproto"
	"github.com/ftkv/v1/router"
	"github.com/ftkv/v1/tool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"gopkg.in/yaml.v3"
	"log"
	"strings"
	"time"
)

type ClientConfigWrapper struct {
	ClientConfig ClientConfig `yaml:"kvClient"`
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

func dialKVClients(serverAddresses []string) []kvproto.KVClient {
	serverCount := len(serverAddresses)
	rpcClients := make([]kvproto.KVClient, serverCount)
	for i := 0; i < serverCount; i++ {
		conn, _ := grpc.Dial(serverAddresses[i], grpc.WithTransportCredentials(insecure.NewCredentials()))
		rpcClients[i] = kvproto.NewKVClient(conn)
	}
	return rpcClients
}

type Client struct {
	lastLeader   int
	timeout      time.Duration
	logEnabled   bool
	routerClient *router.ProxyClient
}

const defaultTimeout = 5000

func MakeClient(configData []byte) (*Client, error) {
	clientConfig, err := readClientConfig(configData)
	if err != nil {
		return nil, &tool.RuntimeError{Stage: "load config", Err: err}
	}
	routerAddresses := clientConfig.RouterAddresses
	routerClient, err := router.MakeProxyClient(routerAddresses, clientConfig.LogEnabled)
	if err != nil {
		return nil, &tool.RuntimeError{Stage: "configure router client", Err: err}
	}
	timeout := clientConfig.Timeout
	if timeout < 0 {
		return nil, errors.New("timeout must be a positive number")
	} else if timeout == 0 {
		log.Printf("configure shardkv client info: using default timeout=%dms", defaultTimeout)
	}
	log.Printf("configure shardkv client info: routerAddresses:%v, timeout=%dms, logEnabled=%v", routerAddresses, timeout, clientConfig.LogEnabled)
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

func (c *Client) queryServersFor(shard int) ([]string, error) {
	clusterConfig, err := c.routerClient.QueryLatest()
	if err != nil {
		return nil, errors.New("get shard info from router: " + err.Error())
	}
	groupId := clusterConfig.ShardsMap[shard]
	serverAddresses := clusterConfig.ReplicaGroups[groupId]
	if serverAddresses == nil || len(serverAddresses) == 0 {
		return nil, errors.New("no server groups alive")
	}
	return serverAddresses, nil
}

func (c *Client) callWithRetry(shard int, args interface{}, handleRPC func(args interface{},
	rpcClient kvproto.KVClient, serverAddress string) (interface{}, bool, error, bool)) (interface{}, error) {

	serverAddresses, err := c.queryServersFor(shard)
	if err != nil {
		return nil, err
	}
	rpcClients := dialKVClients(serverAddresses)

	serverCount := len(rpcClients)
	s := tool.ChooseServer(c.lastLeader, serverCount)
	rpcClient := rpcClients[s]
	retried := false
	for i := 0; i < serverCount; i++ {
		ret, ok, e, wrongGroup := handleRPC(args, rpcClient, serverAddresses[s])
		if ok {
			c.lastLeader = s
			return ret, nil
		}
		if wrongGroup {
			serverAddresses, err = c.queryServersFor(shard)
			if err != nil {
				return nil, err
			}
			if !retried {
				retried = true
				serverCount = len(rpcClients)
				i = -1
			}
			rpcClients = dialKVClients(serverAddresses)
			s = 0
		}
		if e != nil {
			return nil, e
		}
		// retry
		s++
		if s >= serverCount {
			s = 0
		}
		rpcClient = rpcClients[s]
	}
	return nil, errors.New("service unavailable for servers:" + strings.Join(serverAddresses, " "))
}

func (c *Client) handleGet(request interface{}, rpcClient kvproto.KVClient, serverAddress string) (interface{}, bool,
	error, bool) {
	getRequest := request.(*kvproto.GetRequest)
	c.logPrintf("send get request:%v to server:%s", getRequest, serverAddress)
	var reply *kvproto.GetReply
	var err error
	{
		ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
		defer cancel()
		reply, err = rpcClient.Get(ctx, getRequest)
	}
	c.logPrintf("receive get reply:%v, err=%v from server:%s", reply, err, serverAddress)
	if err != nil {
		s, _ := status.FromError(err)
		if s.Code() == codes.DeadlineExceeded {
			return nil, false, errors.New("service unavailable"), false
		}
	} else {
		if reply.ErrCode == kvproto.ResponseCode_OK || reply.ErrCode == kvproto.ResponseCode_KEY_NOT_EXISTS {
			return &getReply{Value: reply.Value, Exist: reply.Exist}, true, nil, false
		}
		if reply.ErrCode == kvproto.ResponseCode_WRONG_GROUP {
			return nil, false, nil, true
		}
	}
	return nil, false, nil, false
}

func (c *Client) handleSet(request interface{}, rpcClient kvproto.KVClient, serverAddress string) (interface{}, bool, error, bool) {
	setRequest := request.(*kvproto.SetRequest)
	c.logPrintf("send set request:%v to server:%s", setRequest, serverAddress)
	var setReply *kvproto.SetReply
	var err error
	{
		ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
		defer cancel()
		setReply, err = rpcClient.Set(ctx, setRequest)
	}
	c.logPrintf("receive set reply:%v, err=%v from server:%s", setReply, err, serverAddress)
	if err != nil {
		s, _ := status.FromError(err)
		if s.Code() == codes.DeadlineExceeded {
			return nil, false, errors.New("service unavailable"), false
		}
	} else {
		if setReply.ErrCode == kvproto.ResponseCode_OK {
			return nil, true, nil, false
		}
		if setReply.ErrCode == kvproto.ResponseCode_WRONG_GROUP {
			return nil, false, nil, true
		}
	}
	return nil, false, nil, false
}

func (c *Client) handleDelete(request interface{}, rpcClient kvproto.KVClient, serverAddress string) (interface{}, bool, error,
	bool) {
	deleteRequest := request.(*kvproto.DeleteRequest)
	c.logPrintf("send delete request:%v to server:%s", deleteRequest, serverAddress)
	var deleteReply *kvproto.DeleteReply
	var err error
	{
		ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
		defer cancel()
		deleteReply, err = rpcClient.Delete(ctx, deleteRequest)
	}
	c.logPrintf("receive delete reply:%v, err=%v from server:%s", deleteReply, err, serverAddress)
	if err != nil {
		s, _ := status.FromError(err)
		if s.Code() == codes.DeadlineExceeded {
			return nil, false, errors.New("service unavailable"), false
		}
	} else {
		if deleteReply.ErrCode == kvproto.ResponseCode_OK {
			return nil, true, nil, false
		}
		if deleteReply.ErrCode == kvproto.ResponseCode_WRONG_GROUP {
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
	req := &kvproto.GetRequest{Key: key}
	r, err := c.callWithRetry(shard, req, c.handleGet)
	if err != nil {
		return "", false, err
	}
	reply := r.(*getReply)
	return reply.Value, reply.Exist, nil
}

func (c *Client) Set(key string, val string) error {
	shard := router.Key2Shard(key)
	req := &kvproto.SetRequest{Key: key, Value: val}
	_, err := c.callWithRetry(shard, req, c.handleSet)
	return err
}

func (c *Client) Delete(key string) error {
	shard := router.Key2Shard(key)
	req := &kvproto.DeleteRequest{Key: key}
	_, err := c.callWithRetry(shard, req, c.handleDelete)
	return err
}
