package kvclient

import (
	"context"
	"errors"
	"github.com.chongchonga/kvservice/v1/common"
	"github.com.chongchonga/kvservice/v1/kvclient/conf"
	"github.com.chongchonga/kvservice/v1/tool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"log"
	"time"
)

import "crypto/rand"
import "math/big"

const DefaultTimeout = 3000 // millisecond

type KVClient struct {
	lastLeader   int
	sessionValid bool
	sessionId    string

	rpcClients []common.KVServerClient
	addresses  []string
	password   string
	timeout    time.Duration
	logEnabled bool
}

func NewClient(configData []byte) (*KVClient, error) {
	clientConf, err := conf.Read(configData)
	if err != nil {
		return nil, err
	}
	serverAddresses := clientConf.Addresses
	err = tool.Check(serverAddresses)
	if err != nil {
		return nil, err
	}
	log.Printf("configure KVClient info: address of servers: %v", serverAddresses)
	timeout := clientConf.Timeout
	if timeout < 0 {
		return nil, errors.New("the timeout must be a positive number")
	} else if timeout == 0 {
		timeout = DefaultTimeout
	}
	ck := new(KVClient)
	ck.lastLeader = -1

	var clients = make([]common.KVServerClient, len(serverAddresses))
	for i, addr := range serverAddresses {
		conn, _ := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		clients[i] = common.NewKVServerClient(conn)
	}
	ck.rpcClients = clients
	ck.addresses = serverAddresses
	ck.password = clientConf.Password
	ck.timeout = time.Duration(timeout) * time.Millisecond
	log.Printf("configure KVClient info: rpc timeout is %dms", timeout)
	if clientConf.LogEnabled {
		ck.logEnabled = true
		log.Println("configure KVClient info: enable log")
	}

	// try to create a session when initialization
	go func() {
		_ = ck.openSession()
	}()
	return ck, nil

}

func (c *KVClient) logPrintf(format string, args ...interface{}) {
	if c.logEnabled {
		log.Printf(format, args...)
	}
}

func (c *KVClient) chooseServer() int {
	lastLeader := c.lastLeader
	if lastLeader != -1 {
		return lastLeader
	} else {
		max := big.NewInt(int64(len(c.rpcClients)))
		bigx, _ := rand.Int(rand.Reader, max)
		x := bigx.Int64()
		return int(x)
	}
}

func (c *KVClient) sendOpenSession(server int, args *common.OpenSessionRequest) (*common.OpenSessionReply, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()
	return c.rpcClients[server].OpenSession(ctx, args)
}

func (c *KVClient) sendGet(server int, args *common.GetRequest) (*common.GetReply, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()
	return c.rpcClients[server].Get(ctx, args)
}

func (c *KVClient) sendUpdate(server int, args *common.UpdateRequest) (*common.UpdateReply, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()
	return c.rpcClients[server].Update(ctx, args)
}

var ErrUnavailable = errors.New("KVServer service unavailable")

var ErrTimeout = errors.New("KVServer request timeout")

func (c *KVClient) openSession() error {
	server := c.chooseServer()
	password := c.password
	args := &common.OpenSessionRequest{
		Password: password,
	}
	serverCount := len(c.rpcClients)
	for i := 0; i < serverCount; i++ {
		address := c.addresses[server]
		c.logPrintf("send OpenSession request to %s, args:{password=%s}", address, password)
		reply, err := c.sendOpenSession(server, args)
		if err == nil {
			sessionId := reply.SessionId
			errCode := reply.ErrCode
			c.logPrintf("client info: receive OpenSession reply from %s, sessionId=%s, errCode=%s", address, sessionId, errCode)
			if errCode == common.ErrCode_OK {
				c.lastLeader = server
				c.sessionId = sessionId
				c.sessionValid = true
				return nil
			}
		} else {
			c.logPrintf("client info: rpc fail, errorInfo:%s", err.Error())
			s, _ := status.FromError(err)
			code := s.Code()
			if codes.DeadlineExceeded == code {
				// cannot reach consensus before timeout
				return ErrTimeout
			}
		}
		server = (server + 1) % serverCount
	}
	return ErrUnavailable
}

//
// Get fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
func (c *KVClient) Get(key string) (string, bool, error) {
	val := ""
	notEmpty := false
	var err error
	if !c.sessionValid {
		err = c.openSession()
		if err != nil {
			return "", false, err
		}
	}
	sessionId := c.sessionId
	args := common.GetRequest{
		Key:       key,
		SessionId: sessionId,
	}

	server := c.chooseServer()
	serverCount := len(c.rpcClients)

	var reply *common.GetReply
	for i := 0; i < serverCount; i++ {
		address := c.addresses[server]
		c.logPrintf("send Get request to %s, args:{key=%s, sessionId=%s}", address, key, sessionId)
		//logInfo("client send get request to server[", server, "] sessionId=", sessionId)
		reply, err = c.sendGet(server, &args)
		if err == nil {
			val = reply.Value
			errCode := reply.ErrCode
			c.logPrintf("receive Get reply from %s, key=%s, value=%s, errCode=%s", address, key, val, errCode)
			if errCode != common.ErrCode_WRONG_LEADER {
				c.lastLeader = server
			}
			if errCode == common.ErrCode_OK || errCode == common.ErrCode_NO_KEY {
				if errCode == common.ErrCode_OK {
					notEmpty = true
				}
				return val, notEmpty, nil
			}
			if errCode == common.ErrCode_INVALID_SESSION {
				err = c.openSession()
				if err != nil {
					return val, notEmpty, err
				}
				sessionId = c.sessionId
				c.sessionValid = true
				args.SessionId = sessionId
			}
		} else {
			c.logPrintf("client info: rpc fail, errorInfo:%s", err.Error())
			s, _ := status.FromError(err)
			code := s.Code()
			if codes.DeadlineExceeded == code {
				// cannot reach consensus before timeout
				return val, notEmpty, ErrTimeout
			}
		}
		// fail
		// switch to next server ...
		server = (server + 1) % serverCount
	}
	return val, notEmpty, ErrUnavailable
}

//
// shared by Put, Append and Delete
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("kvdb.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (c *KVClient) update(key string, value string, op common.Op) error {
	var err error
	if !c.sessionValid {
		err = c.openSession()
		if err != nil {
			return err
		}
	}
	sessionId := c.sessionId
	args := common.UpdateRequest{
		Key:       key,
		Value:     value,
		Op:        op,
		SessionId: sessionId,
	}
	server := c.chooseServer()
	serverCount := len(c.rpcClients)
	var reply *common.UpdateReply
	for i := 0; i < serverCount; i++ {
		address := c.addresses[server]
		c.logPrintf("send %s request to %s, args:{key=%s, sessionId=%s}", op.String(), address, key, sessionId)
		reply, err = c.sendUpdate(server, &args)
		if err == nil {
			errCode := reply.ErrCode
			c.logPrintf("receive %s reply from %s, key=%s, errCode=%s", op.String(), address, key, errCode)
			if errCode == common.ErrCode_OK {
				c.lastLeader = server
				return nil
			}
			if errCode == common.ErrCode_INVALID_SESSION {
				err = c.openSession()
				if err != nil {
					return err
				}
				sessionId = c.sessionId
				args.SessionId = sessionId
			}
		} else {
			c.logPrintf("client info: rpc fail, errorInfo:%s", err.Error())
			s, _ := status.FromError(err)
			code := s.Code()
			if codes.DeadlineExceeded == code {
				// cannot reach consensus before timeout
				return ErrTimeout
			}
		}

		// fail
		// switch to next server ...
		server = (server + 1) % serverCount
	}
	return ErrUnavailable
}

func (c *KVClient) Put(key string, value string) error {
	return c.update(key, value, common.Op_PUT)
}
func (c *KVClient) Append(key string, value string) error {
	return c.update(key, value, common.Op_APPEND)
}
func (c *KVClient) Delete(key string) error {
	return c.update(key, "", common.Op_DELETE)
}
