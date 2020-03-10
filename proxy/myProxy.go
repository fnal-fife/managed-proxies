package proxy

import (
	"context"
	"time"
)

// StoreInMyProxyer encapsulates the method to store an object on a myProxy server
type storeInMyProxyer interface {
	storeInMyProxy(ctx context.Context, retrievers, server string, valid time.Duration) error
}

// StoreInMyProxy uploads a StoreInMyProxyer object on a myproxy server
func StoreInMyProxy(ctx context.Context, sp storeInMyProxyer, retrievers, server string, valid time.Duration) error {
	return sp.storeInMyProxy(ctx, retrievers, server, valid)
}
