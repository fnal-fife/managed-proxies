package experimentutil

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

const (
	goodhost string = "www.google.com"
	badhost  string = "thisisafakehostandwillneverbeusedever.example.com"
)

type goodNode string

func (g goodNode) pingNode(ctx context.Context, p PingConfig) error {
	return nil
}

type badNode string

func (b badNode) pingNode(ctx context.Context, p PingConfig) error {
	time.Sleep(1 * time.Second)
	if e := ctx.Err(); e != nil {
		return e
	}
	return fmt.Errorf("exit status 2 ping: unknown host %s", b)
}

func TestPingNode(t *testing.T) {
	ctx := context.Background()

	var p = PingConfig{
		"foo": "bar",
	}

	// Control test
	if testing.Verbose() {
		t.Log("Running control test")
	}
	g := node(goodhost)
	if err := g.pingNode(ctx, p); err != nil {
		t.Errorf("Expected error to be nil but got %s", err)
		t.Errorf("Our \"reliable\" host, %s, is probably down or just not responding", goodhost)
	}

	// Bad host
	if testing.Verbose() {
		t.Log("Running bogus host test")
	}
	b := node(badhost)
	if err := b.pingNode(ctx, p); err != nil {
		lowerErr := strings.ToLower(err.Error())
		if !strings.Contains(lowerErr, "unknown host") && !strings.Contains(lowerErr, badhost) {
			t.Errorf("Expected error message containing the phrase \"unknown host\" and %s but got %s", badhost, err)
		}
	} else {
		t.Error("Expected some error.  Didn't get any")
	}

	// Timeout
	if testing.Verbose() {
		t.Log("Running timeout test")
	}
	timeoutCtx, cancelTimeout := context.WithTimeout(ctx, time.Duration(1*time.Nanosecond))
	if err := g.pingNode(timeoutCtx, p); err != nil {
		lowerErr := strings.ToLower(err.Error())
		expectedMsg := "context deadline exceeded"
		if lowerErr != expectedMsg {
			t.Errorf("Expected error message to be %s.  Got %s instead", expectedMsg, lowerErr)
		}
	} else {
		t.Error("Expected some timeout error.  Didn't get any")
	}
	cancelTimeout()
}

func TestPingAllNodes(t *testing.T) {
	var i, j int
	numGood := 2
	numBad := 1
	ctx := context.Background()

	var p = PingConfig{
		"foo": "bar",
	}

	if testing.Verbose() {
		t.Logf("Ping all nodes - %d good, %d bad", numGood, numBad)
	}
	pingChannel := pingAllNodes(ctx, p, goodNode(""), badNode(badhost), goodNode(""))

	for n := range pingChannel {
		if n.err != nil {
			j++
		} else {
			i++
		}
	}

	if i != numGood || j != numBad {
		t.Errorf("Expected %d good, %d bad nodes.  Got %d good, %d bad instead.", numGood, numBad, i, j)
	}

	// Timeout
	if testing.Verbose() {
		t.Log("Running timeout test")
	}
	timeoutCtx, cancelTimeout := context.WithTimeout(ctx, time.Duration(1*time.Nanosecond))
	pingChannel = pingAllNodes(timeoutCtx, p, badNode(badhost))
	for n := range pingChannel {
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
