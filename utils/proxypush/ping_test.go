package proxypush

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

func (g goodNode) PingNode(ctx context.Context) error {
	fmt.Println("Running fake PingNode")
	return nil
}

func (g goodNode) String() string { return string(g) }

type badNode string

func (b badNode) PingNode(ctx context.Context) error {
	time.Sleep(1 * time.Second)
	if e := ctx.Err(); e != nil {
		return e
	}
	return fmt.Errorf("exit status 2 ping: unknown host %s", b)
}

func (b badNode) String() string { return string(b) }

// Tests
// TestPingNode pings a PingNoder and makes sure we get the error we expect
func TestPingNode(t *testing.T) {
	ctx := context.Background()

	// Control test
	if testing.Verbose() {
		t.Log("Running control test")
	}
	g := Node(goodhost)
	if err := g.PingNode(ctx); err != nil {
		t.Errorf("Expected error to be nil but got %s", err)
		t.Errorf("Our \"reliable\" host, %s, is probably down or just not responding", goodhost)
	}

	// Bad host
	if testing.Verbose() {
		t.Log("Running bogus host test")
	}
	b := Node(badhost)
	if err := b.PingNode(ctx); err != nil {
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
	if err := g.PingNode(timeoutCtx); err != nil {
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

// TestPingAllNodes makes sure that PingAllNodes returns the errors we expect based on valid/invalid input
// We also make sure that the number of successes and failures reported matches what we expect
func TestPingAllNodes(t *testing.T) {
	var i, j int
	numGood := 2
	numBad := 1
	ctx := context.Background()

	if testing.Verbose() {
		t.Logf("Ping all nodes - %d good, %d bad", numGood, numBad)
	}
	pingChannel := PingAllNodes(ctx, goodNode(""), badNode(badhost), goodNode(""))

	for n := range pingChannel {
		if n.Err != nil {
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
	pingChannel = PingAllNodes(timeoutCtx, badNode(badhost))
	for n := range pingChannel {
		if n.Err != nil {
			lowerErr := strings.ToLower(n.Err.Error())
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
