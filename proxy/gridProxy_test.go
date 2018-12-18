package proxy

import (
	"context"
	"errors"
	//	"fmt"
	//	"os"
	//	"path/filepath"
	"testing"
	"time"
)

// Satisfy GridProxyer interface
type badServiceCert struct{}

func (b *badServiceCert) getGridProxy(ctx context.Context, valid time.Duration) (*GridProxy, error) {
	return nil, errors.New("This failed for some reason")
}

type goodServiceCert struct{}

func (g *goodServiceCert) getGridProxy(ctx context.Context, valid time.Duration) (*GridProxy, error) {
	return &GridProxy{}, nil
}

func TestNewGridProxy(t *testing.T) {
	tests := []struct {
		g   GridProxyer
		err error
	}{
		{
			g:   &badServiceCert{},
			err: errors.New("Could not run grid-proxy-init on service cert: This failed for some reason"),
		},
		{
			g:   &goodServiceCert{},
			err: nil,
		},
	}

	ctx := context.Background()
	valid, _ := time.ParseDuration(defaultValidity)
	for _, test := range tests {
		if _, err := NewGridProxy(ctx, test.g, valid); errorString(err) != errorString(test.err) {
			t.Errorf("NewGridProxy test should have returned %s; got %s instead", test.err, err)
		}
	}

}

//var exePaths map[string]bool
//
//func init() {
//	executables := []string{"grid-proxy-init", "myproxy-store"}
//	const tmpLocation = "/tmp"
//	fmt.Println("Doing testing init first")
//
//	os.Setenv("PATH", tmpLocation)
//	for _, exe := range executables {
//		exePath := filepath.Join(tmpLocation, exe)
//		if _, err := os.Stat(exePath); os.IsNotExist(err) {
//			if _, err := os.Create(exePath); err != nil {
//				panic(fmt.Sprintf("Could not get %s executable:  could not create it in tmp location", exe))
//			}
//			exePaths[exePath] = true
//		} else {
//			exePaths[exePath] = false
//		}
//	}
//}
//
//func TestMain(m *testing.M) {
//
//	rc := m.Run()
//
//	for exe, created := range exePaths {
//		if created {
//			if err := os.Remove(exe); err != nil {
//				panic(fmt.Sprintf("Could not remove executable %s", exe))
//			}
//		}
//	}
//
//	os.Exit(rc)
//}
