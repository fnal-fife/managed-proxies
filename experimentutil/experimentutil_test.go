package experimentutil

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

const (
	goodhost    string = "www.google.com"
	badhost     string = "thisisafakehostandwillneverbeusedever.example.com"
	badNodeMsgf string = "Node %s didn't respond to pings earlier - " +
		"so it's expected that copying there would fail. " +
		"It may be necessary for the experiment to request via a " +
		"ServiceNow ticket that the Scientific Server Infrastructure " +
		"group reboot the node."
)

func TestPingNode(t *testing.T) {
	ctx := context.Background()

	// Control test
	if testing.Verbose() {
		t.Log("Running control test")
	}
	g := node(goodhost)
	if err := g.pingNode(ctx); err != nil {
		t.Errorf("Expected error to be nil but got %s", err)
		t.Errorf("Our \"reliable\" host, %s, is probably down or just not responding", goodhost)
	}

	// Bad host
	if testing.Verbose() {
		t.Log("Running bogus host test")
	}
	b := node(badhost)
	if err := b.pingNode(ctx); err != nil {
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
	if err := g.pingNode(timeoutCtx); err != nil {
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

	if testing.Verbose() {
		t.Logf("Ping all nodes - %d good, %d bad", numGood, numBad)
	}
	pingChannel := pingAllNodes(ctx, goodNode(""), badNode(badhost), goodNode(""))

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
	pingChannel = pingAllNodes(timeoutCtx, badNode(badhost))
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

// Tests:

// Copying proxies succeeds, fails
// Copy proxy succeeds, chmod fails
// Timeout Copy Proxy
// Copy Proxy succeeds, Timeout Chmod proxy
// Copy Proxy Fails, badnode false
// copy proxy fails, badnode true

// proxyTransferInfo contains the information needed to describe where on the source host to find the proxy, and where on the destination host to push that proxy
type goodProxyTransferInfo struct {
	account           string
	role              string
	node              string
	nodeDown          bool
	proxyFileName     string
	proxyFilePathSrc  string
	proxyFileNameDest string
}

func (g *goodProxyTransferInfo) createCopyProxiesStatus() copyProxiesStatus {
	return copyProxiesStatus{g.node, g.account, g.role, nil}
}

func (g *goodProxyTransferInfo) copyProxy(context.Context, []string) error {
	return nil
}

func (g *goodProxyTransferInfo) chmodProxy(context.Context, []string) error {
	return nil
}

type badProxyTransferInfo struct {
	account           string
	role              string
	node              string
	nodeDown          bool
	proxyFileName     string
	proxyFilePathSrc  string
	proxyFileNameDest string
}

func (b *badProxyTransferInfo) createCopyProxiesStatus() copyProxiesStatus {
	return copyProxiesStatus{b.node, b.account, b.role, nil}
}

func (b *badProxyTransferInfo) copyProxy(ctx context.Context, opts []string) error {
	time.Sleep(1 * time.Second)
	if e := ctx.Err(); e != nil {
		return e
	}
	return fmt.Errorf("Copying proxy %s to node %s failed.  The error was test error. %s",
		b.proxyFileName, b.node, b.generateBadNodeMsg())
}

func (b *badProxyTransferInfo) chmodProxy(ctx context.Context, opts []string) error {
	time.Sleep(1 * time.Second)
	if e := ctx.Err(); e != nil {
		return e
	}
	return fmt.Errorf("Error changing permission of proxy %s to mode 400 on %s."+
		"The error was test error.  %s", b.proxyFileName, b.node, b.generateBadNodeMsg())
}

func (b *badProxyTransferInfo) generateBadNodeMsg() (msg string) {
	if b.nodeDown {
		msg = "\n" + fmt.Sprintf(badNodeMsgf, b.node)
	}
	return
}

type copyPassChmodFailProxyTransferInfo struct {
	account           string
	role              string
	node              string
	nodeDown          bool
	proxyFileName     string
	proxyFilePathSrc  string
	proxyFileNameDest string
}

func (b *copyPassChmodFailProxyTransferInfo) createCopyProxiesStatus() copyProxiesStatus {
	return copyProxiesStatus{b.node, b.account, b.role, nil}
}

func (b *copyPassChmodFailProxyTransferInfo) copyProxy(ctx context.Context, s []string) error {
	time.Sleep(1 * time.Second)
	if e := ctx.Err(); e != nil {
		return e
	}
	return nil
}

func (b *copyPassChmodFailProxyTransferInfo) chmodProxy(ctx context.Context, s []string) error {
	time.Sleep(1 * time.Second)
	// Purposefully not checking context here
	return fmt.Errorf("Error changing permission of proxy %s to mode 400 on %s."+
		"The error was test error.  %s", b.proxyFileName, b.node, b.generateBadNodeMsg())
}

func (b *copyPassChmodFailProxyTransferInfo) generateBadNodeMsg() (msg string) {
	if b.nodeDown {
		msg = "\n" + fmt.Sprintf(badNodeMsgf, b.node)
	}
	return
}

func TestCopyProxies(t *testing.T) {
	var i, j int
	numGood := 2
	numBad := 1
	ctx := context.Background()
	var g1, g2 goodProxyTransferInfo
	var b badProxyTransferInfo
	var gb copyPassChmodFailProxyTransferInfo

	if testing.Verbose() {
		t.Logf("Testing mocking pushing voms proxies - %d successful, %d bad.", numGood, numBad)
	}
	copyChannel := copyProxies(ctx, &g1, &b, &g2)
	for c := range copyChannel {
		if c.err != nil {
			j++
		} else {
			i++
		}
	}
	if i != numGood || j != numBad {
		t.Errorf("Expected %d good, %d bad proxies.  Got %d good, %d bad instead.", numGood, numBad, i, j)
	}

	if testing.Verbose() {
		t.Log("Testing mocking copy fails")
	}
	expectedMsgPart := "Copying proxy"
	copyChannel = copyProxies(ctx, &b)
	c := <-copyChannel
	if !strings.Contains(c.err.Error(), expectedMsgPart) {
		t.Errorf("Expected copy to fail, and thus error should have contained the phrase %s.  Got %s instead", expectedMsgPart, c.err)
	}

	if testing.Verbose() {
		t.Log("Testing mocking copy succeeds but chmod fails")
	}
	expectedMsgPart = "Error changing permission of proxy"
	copyChannel = copyProxies(ctx, &gb)
	c = <-copyChannel
	if !strings.Contains(c.err.Error(), expectedMsgPart) {
		t.Errorf("Expected chmod to fail, and thus error should have contained the phrase %s.  Got %s instead", expectedMsgPart, c.err)
	}

	if testing.Verbose() {
		t.Log("Testing mocking copy fails but bad node")
	}
	b.nodeDown = true
	expectedMsgPart1 := "Copying proxy"
	expectedMsgPart2 := fmt.Sprintf(badNodeMsgf, gb.node)
	copyChannel = copyProxies(ctx, &b)
	c = <-copyChannel
	if !(strings.Contains(c.err.Error(), expectedMsgPart1) && strings.Contains(c.err.Error(), expectedMsgPart2)) {
		t.Errorf("Expected chmod to fail, and thus error should have contained the phrases %s and %s.  Got %s instead", expectedMsgPart1, expectedMsgPart2, c.err)
	}
	b.nodeDown = false

	if testing.Verbose() {
		t.Log("Testing mocking copy proxy - timeout")
	}
	timeoutCtx, cancelTimeout := context.WithTimeout(ctx, time.Duration(1*time.Nanosecond))
	copyChannel = copyProxies(timeoutCtx, &b)
	c = <-copyChannel
	if c.err != nil {
		lowerErr := strings.ToLower(c.err.Error())
		expectedMsg := "context deadline exceeded"
		if lowerErr != expectedMsg {
			t.Errorf("Expected error message to be %s.  Got %s instead", expectedMsg, lowerErr)
		}
	} else {
		t.Error("Expected some timeout error.  Didn't get any")
	}
	cancelTimeout()

	if testing.Verbose() {
		t.Log("Testing mocking copy proxy - copy works, but chmod times out")
	}
	timeoutCtx, cancelTimeout = context.WithTimeout(ctx, time.Duration(1*time.Nanosecond))
	copyChannel = copyProxies(timeoutCtx, &gb)
	c = <-copyChannel
	if c.err != nil {
		lowerErr := strings.ToLower(c.err.Error())
		expectedMsg := "context deadline exceeded"
		if lowerErr != expectedMsg {
			t.Errorf("Expected error message to be %s.  Got %s instead", expectedMsg, lowerErr)
		}
	} else {
		t.Error("Expected some timeout error.  Didn't get any")
	}
	cancelTimeout()

}

type goodNode string

func (g goodNode) pingNode(ctx context.Context) error {
	return nil
}

type badNode string

func (b badNode) pingNode(ctx context.Context) error {
	time.Sleep(1 * time.Second)
	if e := ctx.Err(); e != nil {
		return e
	}
	return fmt.Errorf("exit status 2 ping: unknown host %s", b)
}

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
