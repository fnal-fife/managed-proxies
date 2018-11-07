package proxyutils

import "time"

type myProxyer interface {
	StoreInMyProxy(server string, valid time.Duration) error
}
