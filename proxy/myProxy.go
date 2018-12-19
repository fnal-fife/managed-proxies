package proxy

import (
	"context"
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"
)

type MyProxyer interface {
	storeInMyProxy(ctx context.Context, retrievers, server string, valid time.Duration) error
}

func StoreInMyProxy(ctx context.Context, m MyProxyer, retrievers, myProxyServer string, valid time.Duration) error {
	err := m.storeInMyProxy(ctx, retrievers, myProxyServer, valid)
	if err != nil {
		log.WithField("myProxyer", fmt.Sprintf("%v", m)).Error("Could not store myProxyer in myproxy")
	}
	return err
}
