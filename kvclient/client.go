package kvclient

import (
	"context"
	"errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"kvraft/common"
	"log"
	"time"
)

import "crypto/rand"
import "math/big"

const LogEnabled = false

func logInfo(v ...any) {
	if LogEnabled {
		log.Println(v...)
	}
}

const DefaultRPCTimeout = 3 * time.Second

type KVClient struct {
	rpcClients   []common.KVServerClient
	serverCount  int
	lastLeader   int
	rpcTimeout   time.Duration
	sessionValid bool
	sessionId    string
}

func NewClient(serverAddresses []string) (*KVClient, error) {
	return NewTimeoutClient(serverAddresses, DefaultRPCTimeout)
}

func NewTimeoutClient(serverAddresses []string, timeout time.Duration) (*KVClient, error) {
	if len(serverAddresses) == 0 {
		return nil, errors.New("empty servers")
	}
	ck := new(KVClient)
	ck.serverCount = len(serverAddresses)
	var clients = make([]common.KVServerClient, ck.serverCount)
	for i, addr := range serverAddresses {
		conn, _ := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		clients[i] = common.NewKVServerClient(conn)
	}
	ck.rpcClients = clients
	ck.lastLeader = -1
	ck.rpcTimeout = timeout
	go func() {
		_ = ck.openSession(ck.chooseServer())
	}()
	return ck, nil

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

func (c *KVClient) sendOpenSession(server int, args *common.OpenSessionRequest) (*common.OpenSessionReply, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.rpcTimeout)
	defer cancel()
	return c.rpcClients[server].OpenSession(ctx, args)
}

func (c *KVClient) sendGet(server int, args *common.GetRequest) (*common.GetReply, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.rpcTimeout)
	defer cancel()
	return c.rpcClients[server].Get(ctx, args)
}

func (c *KVClient) sendUpdate(server int, args *common.UpdateRequest) (*common.UpdateReply, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.rpcTimeout)
	defer cancel()
	return c.rpcClients[server].Update(ctx, args)
}

var ErrUnavailable = errors.New("KVServer service unavailable")

var ErrTimeout = errors.New("KVServer request timeout")

func (c *KVClient) openSession(server int) error {
	if server > c.serverCount || server < 0 {
		server = c.chooseServer()
	}
	args := &common.OpenSessionRequest{}
	maxFail := c.serverCount
	for i := 0; i < maxFail; i++ {
		reply, err := c.sendOpenSession(server, args)
		if err == nil {
			if reply.ErrCode == common.ErrCode_OK {
				sessionId := reply.SessionId
				c.sessionId = reply.SessionId
				c.sessionValid = true
				c.lastLeader = server
				logInfo("client open a new session, sessionId=", sessionId)
				return nil
			}
		} else {
			logInfo(err)
			s, _ := status.FromError(err)
			code := s.Code()
			if codes.DeadlineExceeded == code {
				// cannot reach consensus before timeout
				return ErrTimeout
			}
		}
		server = (server + 1) % c.serverCount
		logInfo("request fail to finish, switch to server[", server, "]")
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
	var e error
	if !c.sessionValid {
		e = c.openSession(c.chooseServer())
		if e != nil {
			return "", false, e
		}
	}
	sessionId := c.sessionId
	args := common.GetRequest{
		Key:       key,
		SessionId: sessionId,
	}

	server := c.chooseServer()
	maxFail := c.serverCount

	var reply *common.GetReply
	for i := 0; i < maxFail; i++ {
		logInfo("client send get request to server[", server, "] sessionId=", sessionId)
		reply, e = c.sendGet(server, &args)
		if e == nil {
			errCode := reply.ErrCode
			logInfo("client receive get response from  server[", server, "] sessionId=", sessionId, "err=", errCode)
			if errCode == common.ErrCode_OK || errCode == common.ErrCode_NO_KEY {
				c.lastLeader = server
				if errCode == common.ErrCode_OK {
					val = reply.Value
					notEmpty = true
				}
				return val, notEmpty, nil
			}
			if errCode == common.ErrCode_INVALID_SESSION {
				e = c.openSession(c.chooseServer())
				if e != nil {
					return val, notEmpty, e
				}
				sessionId = c.sessionId
				args.SessionId = sessionId
			}
		} else {
			logInfo(e)
			s, _ := status.FromError(e)
			code := s.Code()
			if codes.DeadlineExceeded == code {
				// cannot reach consensus before timeout
				return val, notEmpty, ErrTimeout
			}
		}
		// fail
		// switch to next server ...
		server = (server + 1) % c.serverCount
		logInfo("request fail to finish, switch to server[", server, "]")
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
	var e error
	if !c.sessionValid {
		e = c.openSession(c.chooseServer())
		if e != nil {
			return e
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
	maxFail := c.serverCount
	var reply *common.UpdateReply
	for i := 0; i < maxFail; i++ {
		reply, e = c.sendUpdate(server, &args)

		if e == nil {
			errCode := reply.ErrCode
			if errCode == common.ErrCode_OK {
				c.lastLeader = server
				// success
				return nil
			}
			if errCode == common.ErrCode_INVALID_SESSION {
				e = c.openSession(c.chooseServer())
				if e != nil {
					return e
				}
				sessionId = c.sessionId
				args.SessionId = sessionId
			}
		} else {
			logInfo(e)
			s, _ := status.FromError(e)
			code := s.Code()
			if codes.DeadlineExceeded == code {
				// cannot reach consensus before timeout
				return ErrTimeout
			}
		}

		// fail
		// switch to next server ...
		server = (server + 1) % c.serverCount
		logInfo("request fail to finish, switch to server[", server, "]")
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
