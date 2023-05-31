package tool

import (
	"net/rpc"
)

// Peer is a rpc client using the go library
type Peer struct {
	addr      string      // peer's address
	rpcClient *rpc.Client // client for rpc call
}

func (p *Peer) Call(serviceMethod string, args any, reply any) error {
	var err error
	if p.rpcClient == nil {
		p.rpcClient, err = rpc.DialHTTP("tcp", p.addr)
	}
	if err == nil {
		err = p.rpcClient.Call(serviceMethod, args, reply)
		if err != nil {
			p.rpcClient = nil
		}
	}
	return err
}

func MakePeer(addr string) *Peer {
	p := new(Peer)
	p.addr = addr
	return p
}

func (p *Peer) GetAddr() string {
	return p.addr
}
