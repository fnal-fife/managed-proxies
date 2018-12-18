package proxy

import (
	"context"
	//"errors"
	//	"fmt"
	//	"os"
	//	"path/filepath"
	"testing"
	"time"
)

type testingError string

func (t testingError) Error() string {
	return string(t)
}

// Satisfy gridProxyer interface
type badServiceCert struct{}

func (b *badServiceCert) getGridProxy(ctx context.Context, valid time.Duration) (*GridProxy, error) {
	return nil, testingError("This failed for some reason")
}

type goodServiceCert struct{}

func (g *goodServiceCert) getGridProxy(ctx context.Context, valid time.Duration) (*GridProxy, error) {
	return &GridProxy{}, nil
}

func TestNewGridProxy(t *testing.T) {
	tests := []struct {
		g   gridProxyer
		err testingError
	}{
		{
			g:   &badServiceCert{},
			err: testingError("Could not run grid-proxy-init on service cert: This failed for some reason"),
		},
		{
			g:   &goodServiceCert{},
			err: testingError(""),
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

func errorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
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
