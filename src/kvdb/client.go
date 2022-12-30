package kvdb

import (
	"kvraft/src/rpc"
	"log"
	"sync/atomic"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers       []*rpc.Peer
	serverCount   int
	lastLeader    int
	ClientId      int64
	nextRequestId int64
}

func MakeClerk(serverAddresses []string) *Clerk {
	ck := new(Clerk)
	var servers []*rpc.Peer
	for _, addr := range serverAddresses {
		p := rpc.MakePeer(addr)
		servers = append(servers, p)
	}
	ck.servers = servers
	// You'll have to add code here.
	ck.serverCount = len(servers)
	ck.lastLeader = -1
	ck.nextRequestId = 1
	ck.openSession()
	return ck
}

func (c *Clerk) openSession() {
	args := &OpenSessionArgs{}
	server := c.chooseServer()
	maxFail := c.serverCount
	for i := 0; i < maxFail; {
		reply := &OpenSessionReply{}
		ok := c.sendOpenSession(server, args, reply)
		if ok {
			if reply.Err == ErrTimeout {
				i++
				continue
			}
			if reply.Err == OK {
				log.Println("clientId is ", reply.ClientId)
				c.lastLeader = server
				c.ClientId = reply.ClientId
				c.nextRequestId = 1
				return
			}
		} else {
			i++
		}
		server = (server + 1) % c.serverCount
	}
	log.Fatalln("can't connect to server!")
}

// 1. 使用UUID
// 2. 使用原子递增的clientID和原子递增的requestId
func (ck *Clerk) getRequestId() int64 {
	for {
		current := ck.nextRequestId
		next := current + 1
		if atomic.CompareAndSwapInt64(&ck.nextRequestId, current, next) {
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

func (c *Clerk) chooseServer() int {
	if c.lastLeader != -1 {
		return c.lastLeader
	} else {
		return randN(c.serverCount)
	}
}

func (ck *Clerk) sendOpenSession(server int, args *OpenSessionArgs, reply *OpenSessionReply) bool {
	return ck.servers[server].Call("KVServer.OpenSession", args, reply)
}

func (ck *Clerk) sendGet(server int, args *GetArgs, reply *GetReply) bool {
	return ck.servers[server].Call("KVServer.Get", args, reply)
}

func (ck *Clerk) sendUpdate(server int, args *UpdateArgs, reply *UpdateReply) bool {
	return ck.servers[server].Call("KVServer.Update", args, reply)
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	args := GetArgs{
		Key:       key,
		ClientId:  ck.ClientId,
		RequestId: ck.getRequestId(),
	}
	server := ck.chooseServer()
	maxFail := ck.serverCount
	for i := 0; i < maxFail; {
		reply := GetReply{}
		log3A("client send get request to server %d, requestId=%d", server, args.RequestId)
		ok := ck.sendGet(server, &args, &reply)
		if ok {
			err := reply.Err
			log3A("client receive get response from server %d, requestId=%d,err=%s", server, args.RequestId, err)
			if err == OK || err == ErrNoKey {
				ck.lastLeader = server
				value := ""
				if err != ErrNoKey {
					value = reply.Value
				}
				return value
			}
			if err == ErrTimeout {
				i++
				continue
			}
			if err == ErrInvalidSession {
				ck.openSession()
			}
		} else {
			i++
		}
		// fail
		// switch to next server ...
		server = (server + 1) % ck.serverCount
		log3A("request fail to finish, switch to next server=%d", server)
	}
	log.Fatalln("can't connect to server!")
	return ""
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
func (ck *Clerk) update(key string, value string, op string) {
	// You will have to modify this function.
	args := UpdateArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		ClientId:  ck.ClientId,
		RequestId: ck.getRequestId(),
	}
	server := ck.chooseServer()
	maxFail := ck.serverCount
	for i := 0; i < maxFail; {
		reply := UpdateReply{}
		log3A("client send get request to server %d, requestId=%d", server, args.RequestId)
		ok := ck.sendUpdate(server, &args, &reply)
		if ok {
			err := reply.Err
			log3A("client receive %v response from server %d, requestId=%d,err=%s", op, server, args.RequestId, err)
			if err == OK {
				ck.lastLeader = server
				// success
				return
			}
			if err == ErrTimeout {
				i++
				continue
			}
			if err == ErrInvalidSession {
				ck.openSession()
			}
		} else {
			i++
		}
		// fail
		// switch to next server ...
		server = (server + 1) % ck.serverCount
		log3A("request fail to finish, switch to next server=%d", server)
	}
	log.Fatalln("can't connect to server!")
}

func (ck *Clerk) Put(key string, value string) {
	ck.update(key, value, PUT)
}
func (ck *Clerk) Append(key string, value string) {
	ck.update(key, value, APPEND)
}
func (ck *Clerk) Delete(key string) {
	ck.update(key, "", DELETE)
}
