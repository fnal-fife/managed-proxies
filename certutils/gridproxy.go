package proxyutils

import "fmt"

type GridProxy struct {
	Path string
	DN   string
}

func getGridProxy(s serviceCert, outfile string) *GridProxy {
	// TODO:  Need to actually do something here
	fmt.Println("vim-go")
	g := GridProxy{outfile, ""}
	return &g
}

func (g *GridProxy) runMyProxyStore(server, retrievers, hours string, s serviceCert) error {
	return nil
}

// Somewhere else, define an interface myProxyer that has a runMyProxyStore func.  Have the func that eventually runs runMyProxyStore run it on the interface.
