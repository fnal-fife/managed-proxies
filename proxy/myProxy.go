package proxy

import (
	"context"
	"time"
)

type MyProxyer interface {
	storeInMyProxy(ctx context.Context, retrievers, server string, valid time.Duration) error
}

func StoreInMyProxy(ctx context.Context, m MyProxyer, retrievers, myProxyServer string, valid time.Duration) error {
	return m.storeInMyProxy(ctx, retrievers, myProxyServer, valid)
}
