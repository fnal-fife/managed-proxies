package experimentutil

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

const badNodeMsgf string = "Node %s didn't respond to pings earlier - " +
	"so it's expected that copying there would fail. " +
	"It may be necessary for the experiment to request via a " +
	"ServiceNow ticket that the Scientific Server Infrastructure " +
	"group reboot the node."

func TestCopyProxiesCount(t *testing.T) {
	var i, j int
	var testPointers []pushProxyer
	numGood := 1
	numBad := 2
	ctx := context.Background()

	// Test cases
	var countTests = []struct {
		f               fakeProxyTransferInfo
		expectedMsgPart string
	}{
		{
			f: fakeProxyTransferInfo{
				tag:               "good transfer",
				copyErr:           nil,
				chmodErr:          nil,
				contextCopyCheck:  false,
				proxyTransferInfo: proxyTransferInfo{},
			},
			expectedMsgPart: "",
		},
		{
			f: fakeProxyTransferInfo{
				tag:               "bad transfer",
				copyErr:           errors.New("Copying proxy to node failed.  The error was test error"),
				chmodErr:          errors.New("Error changing permission of proxy to mode 400. The error was test error"),
				contextCopyCheck:  false,
				proxyTransferInfo: proxyTransferInfo{},
			},
			expectedMsgPart: "Copying proxy",
		},
		{
			f: fakeProxyTransferInfo{
				tag:               "bad transfer",
				copyErr:           errors.New("Copying proxy to node failed.  The error was test error"),
				chmodErr:          errors.New("Error changing permission of proxy to mode 400. The error was test error"),
				contextCopyCheck:  false,
				proxyTransferInfo: proxyTransferInfo{},
			},
			expectedMsgPart: "Error changing permission of proxy",
		},
	}

	// Tests
	if testing.Verbose() {
		t.Logf("Testing mocking pushing voms proxies - %d successful, %d bad.", numGood, numBad)
	}

	for idx := range countTests {
		testPointers = append(testPointers, &countTests[idx].f)
	}

	copyChannel := copyProxies(ctx, testPointers...)

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
}

func TestCopyProxiesBasic(t *testing.T) {
	ctx := context.Background()

	// Test cases
	var basicTests = []struct {
		f               fakeProxyTransferInfo
		expectedMsgPart string
	}{
		{
			f: fakeProxyTransferInfo{
				tag:               "copy fails",
				copyErr:           errors.New("Copying proxy to node failed.  The error was test error"),
				chmodErr:          nil,
				contextCopyCheck:  false,
				proxyTransferInfo: proxyTransferInfo{},
			},
			expectedMsgPart: "Copying proxy",
		},
		{
			f: fakeProxyTransferInfo{
				tag:               "copy succeeds, chmod fails",
				copyErr:           nil,
				chmodErr:          errors.New("Error changing permission of proxy to mode 400. The error was test error"),
				contextCopyCheck:  false,
				proxyTransferInfo: proxyTransferInfo{},
			},
			expectedMsgPart: "Error changing permission of proxy",
		},
	}

	// Test loop
	for _, test := range basicTests {
		if testing.Verbose() {
			t.Log(test.f.tag)
		}
		copyChannel := copyProxies(ctx, &test.f)
		c := <-copyChannel
		if !strings.Contains(c.err.Error(), test.expectedMsgPart) {
			t.Errorf("Expected state \"%s\", and thus error should have contained the phrase %s.  Got %s instead", test.f.tag, test.expectedMsgPart, c.err)
		}
	}
}

func TestBadNode(t *testing.T) {
	var f fakeProxyTransferInfo
	ctx := context.Background()

	if testing.Verbose() {
		t.Log("Testing mocking copy fails but bad node")
	}
	f.copyErr = errors.New("Copying proxy to node failed.  The error was test error")
	f.nodeDown = true
	expectedMsgPart1 := "Copying proxy"
	expectedMsgPart2 := fmt.Sprintf(badNodeMsgf, f.node)
	copyChannel := copyProxies(ctx, &f)
	c := <-copyChannel
	if !(strings.Contains(c.err.Error(), expectedMsgPart1) && strings.Contains(c.err.Error(), expectedMsgPart2)) {
		t.Errorf("Expected chmod to fail, and thus error should have contained the phrases %s and %s.  Got %s instead", expectedMsgPart1, expectedMsgPart2, c.err)
	}
}

func TestTimeouts(t *testing.T) {
	ctx := context.Background()

	// Test cases
	var timeoutTests = []struct {
		f           fakeProxyTransferInfo
		expectedMsg string
	}{
		{
			f: fakeProxyTransferInfo{
				tag:               "copy timeout",
				copyErr:           errors.New("Copying proxy to node failed.  The error was test error"),
				chmodErr:          nil,
				contextCopyCheck:  true,
				proxyTransferInfo: proxyTransferInfo{},
			},
			expectedMsg: "context deadline exceeded",
		},
		{
			f: fakeProxyTransferInfo{
				tag:               "copy succeeds, chmod times out",
				copyErr:           nil,
				chmodErr:          errors.New("Error changing permission of proxy to mode 400. The error was test error"),
				contextCopyCheck:  false,
				proxyTransferInfo: proxyTransferInfo{},
			},
			expectedMsg: "context deadline exceeded",
		},
	}

	// Test loop
	for _, test := range timeoutTests {
		if testing.Verbose() {
			t.Log(test.f.tag)
		}

		timeoutCtx, cancelTimeout := context.WithTimeout(ctx, time.Duration(1*time.Nanosecond))
		copyChannel := copyProxies(timeoutCtx, &test.f)
		c := <-copyChannel
		if c.err != nil {
			lowerErr := strings.ToLower(c.err.Error())
			if lowerErr != test.expectedMsg {
				t.Errorf("Expected error message to be %s.  Got %s instead", test.expectedMsg, lowerErr)
			}
		} else {
			t.Error("Expected some timeout error.  Didn't get any error")
		}
		cancelTimeout()
	}
}

type fakeProxyTransferInfo struct {
	tag              string
	copyErr          error
	chmodErr         error
	contextCopyCheck bool
	proxyTransferInfo
}

func (f *fakeProxyTransferInfo) createCopyProxiesStatus() copyProxiesStatus {
	return copyProxiesStatus{f.node, f.account, f.role, nil}
}

func (f *fakeProxyTransferInfo) copyProxy(ctx context.Context, opts []string) error {
	time.Sleep(1 * time.Second)
	if f.contextCopyCheck {
		if e := ctx.Err(); e != nil {
			return e
		}
	}
	if f.copyErr != nil {
		return errors.New(f.copyErr.Error() + f.generateBadNodeMsg())
	}
	return nil
}

func (f *fakeProxyTransferInfo) chmodProxy(ctx context.Context, opts []string) error {
	time.Sleep(1 * time.Second)
	// We want to always check context here
	if e := ctx.Err(); e != nil {
		return e
	}
	return f.chmodErr
}

func (f *fakeProxyTransferInfo) generateBadNodeMsg() (msg string) {
	if f.nodeDown {
		msg = "\n" + fmt.Sprintf("Node %s didn't respond to pings earlier - "+
			"so it's expected that copying there would fail. "+
			"It may be necessary for the experiment to request via a "+
			"ServiceNow ticket that the Scientific Server Infrastructure "+
			"group reboot the node.", f.node)
	}
	return
}
