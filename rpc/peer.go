package rpc

import (
	"log"
	"net/rpc"
)

// Peer is a rpc client using the go library
type Peer struct {
	addr      string      // peer's address
	rpcClient *rpc.Client // client for rpc call
}

func (p *Peer) Call(serviceMethod string, args any, reply any) bool {
	var err error
	if p.rpcClient == nil {
		p.rpcClient, err = rpc.DialHTTP("tcp", p.addr)
	}
	if err != nil {
		log.Println(err)
	} else {
		err = p.rpcClient.Call(serviceMethod, args, reply)
		if err != nil {
			p.rpcClient = nil
		}
	}
	return err == nil
}

func MakePeer(addr string) *Peer {
	p := new(Peer)
	p.addr = addr
	return p
}
