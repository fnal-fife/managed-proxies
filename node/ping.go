package node

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"sync"
	"text/template"

	"cdcvs.fnal.gov/discompsupp/ken_proxy_push/utils"
)

const pingArgs = "-W 5 -c 1 {{.Node}}"

var (
	pingExecutables = map[string]string{
		"ping": "",
	}
	pingTemplate = template.Must(template.New("ping").Parse(pingArgs))
)

// PingNoder is an interface that wraps the pingNode method. It is meant to be used where pinging a node is necessary
type PingNoder interface {
	pingNode(context.Context) error
}

type node string

// pingNode pings a node (described by a node object) with a 5-second timeout.  It returns an error
func (n node) pingNode(ctx context.Context) error {
	var b strings.Builder
	var pArgs = map[string]string{
		"Node": string(n),
	}

	if err := pingTemplate.Execute(&b, pArgs); err != nil {
		return fmt.Errorf("Could not execute ping template: %s", err.Error())
	}

	args, err := utils.GetArgsFromTemplate(b.String())
	if err != nil {
		return fmt.Errorf("Could not get ping command arguments from template: %s", err.Error())
	}

	cmd := exec.CommandContext(ctx, pingExecutables["ping"], args...)
	if cmdOut, cmdErr := cmd.CombinedOutput(); cmdErr != nil {
		if e := ctx.Err(); e != nil {
			return e
		}
		return fmt.Errorf("%s %s", cmdErr, cmdOut)
	}
	return nil
}

// pingNodeStatus stores information about an attempt to ping a node.  If there was an error, it's stored in err.
type pingNodeStatus struct {
	PingNoder
	err error
}

// pingAllNodes will launch goroutines, which each ping a node the PingNoder variadic nodes.  It returns a channel,
// on which it reports the pingNodeStatuses signifying success or error
func PingAllNodes(ctx context.Context, nodes ...PingNoder) <-chan pingNodeStatus {
	// Buffered Channel to report on
	c := make(chan pingNodeStatus, len(nodes))

	var wg sync.WaitGroup
	wg.Add(len(nodes))

	for _, n := range nodes {
		go func(n PingNoder) {
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

func init() {
	if err := utils.CheckForExecutables(pingExecutables); err != nil {
		panic(err)
	}
}
