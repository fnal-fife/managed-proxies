package proxy

import (
	"context"
	"time"
)

type myProxyer interface {
	StoreInMyProxy(ctx context.Context, server string, valid time.Duration) error
}
