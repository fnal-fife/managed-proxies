package experimentutil

import (
	"context"
	"fmt"
	"os/exec"
	"sync"
)

type pingNoder interface {
	pingNode(context.Context) error
}

type node string

// pingNodeStatus stores information about an attempt to ping a node.  If there was an error, it's stored in err.
type pingNodeStatus struct {
	pingNoder
	err error
}

// pingAllNodes will launch goroutines, which each ping a node the variadic nodes.  It returns a channel,
// on which it reports the pingNodeStatuses signifying success or error
func pingAllNodes(ctx context.Context, nodes ...pingNoder) <-chan pingNodeStatus {
	// Buffered Channel to report on
	c := make(chan pingNodeStatus, len(nodes))

	var wg sync.WaitGroup
	wg.Add(len(nodes))

	for _, n := range nodes {
		go func(n pingNoder) {
			defer wg.Done()
			p := pingNodeStatus{n, n.pingNode(ctx)}
			c <- p
		}(n)
	}

	// Wait for all goroutines to finish, then close channel so that expt Worker can proceed
	go func() {
		defer close(c)
		wg.Wait()
	}()

	return c
}

// pingNode pings a node (described by a node object) with a 5-second timeout.  It returns an error
func (n node) pingNode(ctx context.Context) error {
	pingargs := []string{"-W", "5", "-c", "1", string(n)}
	cmd := exec.CommandContext(ctx, "ping", pingargs...)
	if cmdOut, cmdErr := cmd.CombinedOutput(); cmdErr != nil {
		if e := ctx.Err(); e != nil {
			return e
		}
		return fmt.Errorf("%s %s", cmdErr, cmdOut)
	}
	return nil
}
