package experimentutil

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

type goodVomsProxy struct{}

func (g *goodVomsProxy) getProxy(ctx context.Context) (string, error) {
	return "/path/to/file", nil
}

type badVomsProxy struct{}

func (b *badVomsProxy) getProxy(ctx context.Context) (string, error) {
	time.Sleep(1 * time.Second)
	if e := ctx.Err(); e != nil {
		return "", e
	}
	err := fmt.Sprintf(`Error obtaining testaccount.testrole.proxy.  Please check the cert on 
		fifeutilgpvm01. \n Continuing on to next role.`)
	return "", errors.New(err)
}

func TestGetProxies(t *testing.T) {
	var i, j int
	numGood := 2
	numBad := 1
	ctx := context.Background()
	var g1, g2 goodVomsProxy
	var b badVomsProxy

	if testing.Verbose() {
		t.Logf("Testing mocking generating voms proxies - %d successful, %d bad.", numGood, numBad)
	}
	vpiChannel := getProxies(ctx, &g1, &b, &g2)
	for p := range vpiChannel {
		if p.err != nil {
			j++
		} else {
			i++
		}
	}

	if i != numGood || j != numBad {
		t.Errorf("Expected %d good, %d bad proxies.  Got %d good, %d bad instead.", numGood, numBad, i, j)
	}

	if testing.Verbose() {
		t.Log("Testing mocking generating voms proxies - timeout")
	}
	timeoutCtx, cancelTimeout := context.WithTimeout(ctx, time.Duration(1*time.Nanosecond))
	vpiChannel = getProxies(timeoutCtx, &b)
	for n := range vpiChannel {
		if n.err != nil {
			lowerErr := strings.ToLower(n.err.Error())
			expectedMsg := "context deadline exceeded"
			if lowerErr != expectedMsg {
				t.Errorf("Expected error message to be %s.  Got %s instead", expectedMsg, lowerErr)
			}
		} else {
			t.Error("Expected some timeout error.  Didn't get any")
		}
	}
	cancelTimeout()

}
