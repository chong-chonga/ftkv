package router

import (
	"github.com/ftkv/v1/router/routerproto"
)

type ProxyClient struct {
	listenerClient *ListenerClient
}

func MakeProxyClient(routerAddresses []string) (*ProxyClient, error) {
	client, err := MakeListenerClient(routerAddresses)
	if err != nil {
		return nil, err
	}

	return &ProxyClient{
		client,
	}, nil
}

func (c *ProxyClient) QueryLatest() (*ClusterConfig, error) {
	return c.listenerClient.Query(routerproto.LatestConfigId)
}
