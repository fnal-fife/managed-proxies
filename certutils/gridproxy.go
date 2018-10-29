package proxyutils

import (
	"fmt"
	"time"
)

type GridProxy struct {
	Path string
	DN   string
	//Valid?
}

func NewGridProxy(s *serviceCert, valid time.Duration) (*GridProxy, error) {
	//TODO implement this.  Want to set time here, maybe.  If so, pass into runGridProxyInit
	// TODO Run checks in this method to make sure we can actually run grid-proxy-init
	// Sanity-check time and set default time if no time given.

	g := s.runGridProxyInit(valid)
	return g, nil
}

func (s *serviceCert) runGridProxyInit(valid time.Duration) *GridProxy {
	// TODO:  Need to actually do grid-proxy-init here
	// Set tempfile here
	fmt.Println("vim-go")
	g := GridProxy{"/tmp/blah", ""}
	return &g
}

func (g *GridProxy) runMyProxyStore(server, retrievers, hours string, s serviceCert) error {
	return nil
}

// Somewhere else, define an interface myProxyer that has a runMyProxyStore func.  Have the func that eventually runs runMyProxyStore run it on the interface.
