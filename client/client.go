package client

import (
	"context"
	"errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"kvraft/proto"
	"log"
	"sync/atomic"
	"time"
)

import "crypto/rand"
import "math/big"

const LogEnabled = true

func logInfo(v ...any) {
	if LogEnabled {
		log.Println(v...)
	}
}

const RPCTimeout = 3 * time.Second

type KVClient struct {
	rpcClients    []kvserver.kvserver
	serverCount   int
	lastLeader    int
	ClientId      int64
	nextRequestId int64
}

func DailClient(serverAddresses []string) *KVClient {
	ck := new(KVClient)
	ck.serverCount = len(serverAddresses)
	var clients = make([]kvserver.KVServerClient, ck.serverCount)
	for i, addr := range serverAddresses {
		conn, _ := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		clients[i] = kvserver.NewKVServerClient(conn)
	}
	ck.rpcClients = clients
	ck.lastLeader = -1
	ck.nextRequestId = 1
	go func() {
		_ = ck.openSession(ck.chooseServer())
	}()
	return ck
}

// 1. 使用UUID
// 2. 使用原子递增的clientID和原子递增的requestId
func (c *KVClient) getRequestId() int64 {
	for {
		current := c.nextRequestId
		next := current + 1
		if atomic.CompareAndSwapInt64(&c.nextRequestId, current, next) {
			return current
		}
	}
}

func randN(n int) int {
	max := big.NewInt(int64(n))
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return int(x)
}

func (c *KVClient) chooseServer() int {
	if c.lastLeader != -1 {
		return c.lastLeader
	} else {
		return randN(c.serverCount)
	}
}

func (c *KVClient) sendOpenSession(server int, args *kvserver.OpenSessionRequest) (*kvserver.OpenSessionReply, error) {
	ctx, cancel := context.WithTimeout(context.Background(), RPCTimeout)
	defer cancel()
	return c.rpcClients[server].OpenSession(ctx, args)
}

func (c *KVClient) sendGet(server int, args *kvserver.GetRequest) (*kvserver.GetReply, error) {
	ctx, cancel := context.WithTimeout(context.Background(), RPCTimeout)
	defer cancel()
	return c.rpcClients[server].Get(ctx, args)
}

func (c *KVClient) sendUpdate(server int, args *kvserver.UpdateRequest) (*kvserver.UpdateReply, error) {
	ctx, cancel := context.WithTimeout(context.Background(), RPCTimeout)
	defer cancel()
	return c.rpcClients[server].Update(ctx, args)
}

func (c *KVClient) openSession(server int) error {
	if server > c.serverCount || server < 0 {
		server = c.chooseServer()
		log.Println("provided server[", server, "]not exists, choose random server instead")
	}
	args := &kvserver.OpenSessionRequest{}
	maxFail := c.serverCount
	var errorMsg string
	timeout := false
	for i := 0; i < maxFail; i++ {
		reply, err := c.sendOpenSession(server, args)
		if err == nil {
			if reply.ErrCode == kvserver.ErrCode_OK {
				logInfo("client open a new session, clientId=", reply.ClientId)
				c.lastLeader = server
				c.ClientId = reply.ClientId
				c.nextRequestId = 1
				return nil
			}
		} else {
			logInfo(err)
			s, _ := status.FromError(err)
			code := s.Code()
			if codes.DeadlineExceeded == code {
				// cannot reach consensus before timeout
				timeout = true
				errorMsg = "request timeout, please try again later"
				break
			}
		}
		server = (server + 1) % c.serverCount
		logInfo("request fail to finish, switch to server[", server, "]")
	}
	if !timeout {
		errorMsg = "error while connecting to the server, please check the server configuration"
	}
	return errors.New(errorMsg)
}

//
// Get fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
func (c *KVClient) Get(key string) (string, error) {
	var e error
	if c.ClientId == -1 {
		e = c.openSession(c.chooseServer())
		if e != nil {
			return "", e
		}
	}
	args := kvserver.GetRequest{
		Key: key,
	}
	server := c.chooseServer()
	maxFail := c.serverCount

	var errorMsg string
	timeout := false
	var reply *kvserver.GetReply
	for i := 0; i < maxFail; i++ {
		logInfo("client send get request to server[", server, "] clientId=", args.ClientId, "requestId=", args.RequestId)
		reply, e = c.sendGet(server, &args)
		if e == nil {
			errCode := reply.ErrCode
			logInfo("client receive get response from  server[", server, "] clientId=", args.ClientId, "requestId=", args.RequestId, "err=", errCode)
			if errCode == kvserver.ErrCode_OK || errCode == kvserver.ErrCode_NO_KEY {
				c.lastLeader = server
				value := ""
				if errCode != kvserver.ErrCode_NO_KEY {
					value = reply.Value
				}
				return value, nil
			}
			if errCode == kvserver.ErrCode_INVALID_SESSION {
				c.ClientId = -1
				return "", errors.New("session expired, please try again")
			}
		} else {
			logInfo(e)
			s, _ := status.FromError(e)
			code := s.Code()
			if codes.DeadlineExceeded == code {
				// cannot reach consensus before timeout
				timeout = true
				errorMsg = "request timeout, please try again later"
				break
			}
		}
		// fail
		// switch to next server ...
		server = (server + 1) % c.serverCount
		logInfo("request fail to finish, switch to server[", server, "]")
	}
	if !timeout {
		errorMsg = "error while connecting to the server, please check the server configuration"
	}
	return "", errors.New(errorMsg)
}

//
// shared by Put, Append and Delete
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (c *KVClient) update(key string, value string, op kvserver.Op) error {
	var e error
	if c.ClientId == -1 {
		e = c.openSession(c.chooseServer())
		if e != nil {
			return e
		}
	}
	args := kvserver.UpdateRequest{
		Key:       key,
		Value:     value,
		Op:        op,
		ClientId:  c.ClientId,
		RequestId: c.getRequestId(),
	}
	server := c.chooseServer()
	maxFail := c.serverCount
	var errorMsg string
	timeout := false
	var reply *kvserver.UpdateReply
	for i := 0; i < maxFail; i++ {
		reply, e = c.sendUpdate(server, &args)

		if e == nil {
			errCode := reply.ErrCode
			if errCode == kvserver.ErrCode_OK {
				c.lastLeader = server
				// success
				return nil
			}
			if errCode == kvserver.ErrCode_INVALID_SESSION {
				c.ClientId = -1
				return errors.New("session expired, please try again")
			}
		} else {
			logInfo(e)
			s, _ := status.FromError(e)
			code := s.Code()
			if codes.DeadlineExceeded == code {
				// cannot reach consensus before timeout
				timeout = true
				errorMsg = "request timeout, please try again later"
				break
			}
		}

		// fail
		// switch to next server ...
		server = (server + 1) % c.serverCount
		logInfo("request fail to finish, switch to server[", server, "]")
	}
	if !timeout {
		errorMsg = "error while connecting to the server, please check the server configuration"
	}
	return errors.New(errorMsg)
}

func (c *KVClient) Put(key string, value string) error {
	return c.update(key, value, kvserver.Op_PUT)
}
func (c *KVClient) Append(key string, value string) error {
	return c.update(key, value, kvserver.Op_APPEND)
}
func (c *KVClient) Delete(key string) error {
	return c.update(key, "", kvserver.Op_DELETE)
}
