package node

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"sync"
	"text/template"

	log "github.com/sirupsen/logrus"

	"cdcvs.fnal.gov/discompsupp/ken_proxy_push/v3/internal/pkg/utils"
)

const pingArgs = "-W 5 -c 1 {{.Node}}"

var (
	pingExecutables = map[string]string{
		"ping": "",
	}
	pingTemplate = template.Must(template.New("ping").Parse(pingArgs))
)

// PingNoder is an interface that wraps the pingNode method. It is meant to be used where pinging a node is necessary.  PingNoders also implement the Stringer interface.
type PingNoder interface {
	PingNode(context.Context) error
	String() string
}

// Node is an interactive node
type Node string

// NewNode returns a Node object when given a string
func NewNode(s string) Node { return Node(s) }

// PingNode pings a node (described by a Node object) with a 5-second timeout.  It returns an error
func (n Node) PingNode(ctx context.Context) error {
	var b strings.Builder
	var pArgs = map[string]string{
		"Node": string(n),
	}

	if err := pingTemplate.Execute(&b, pArgs); err != nil {
		err := fmt.Sprintf("Could not execute ping template: %s", err.Error())
		log.Error(err)
		return errors.New(err)
	}

	args, err := utils.GetArgsFromTemplate(b.String())
	if err != nil {
		err := fmt.Sprintf("Could not get ping command arguments from template: %s", err.Error())
		log.WithField("templateStringsBuilder", b.String()).Error(err)
		return errors.New(err)
	}

	cmd := exec.CommandContext(ctx, pingExecutables["ping"], args...)
	if cmdOut, cmdErr := cmd.CombinedOutput(); cmdErr != nil {
		if e := ctx.Err(); e != nil {
			log.WithField("command", strings.Join(cmd.Args, " ")).Error(fmt.Sprintf("Context error: %s", e.Error()))
			return e
		}
		log.WithField("command", strings.Join(cmd.Args, " ")).Error(fmt.Sprintf("Error running ping command: %s %s", string(cmdOut), cmdErr.Error()))
		return fmt.Errorf("%s %s", cmdOut, cmdErr)
	}
	return nil
}

// String converts a Node object into a string
func (n Node) String() string { return string(n) }

// PingNodeStatus stores information about an attempt to ping a Node.  If there was an error, it's stored in Err.
type PingNodeStatus struct {
	PingNoder
	Err error
}

// PingAllNodes will launch goroutines, which each ping a PingNoder from the nodes variadic.  It returns a channel,
// on which it reports the pingNodeStatuses signifying success or error
func PingAllNodes(ctx context.Context, nodes ...PingNoder) <-chan PingNodeStatus {
	// Buffered Channel to report on
	c := make(chan PingNodeStatus, len(nodes))

	var wg sync.WaitGroup
	wg.Add(len(nodes))

	for _, n := range nodes {
		go func(n PingNoder) {
			defer wg.Done()
			p := PingNodeStatus{n, n.PingNode(ctx)}
			c <- p
		}(n)
	}

	// Wait for all goroutines to finish, then close channel so that the caller knows all objects have been sent
	go func() {
		defer close(c)
		wg.Wait()
	}()

	return c
}

func init() {
	if err := utils.CheckForExecutables(pingExecutables); err != nil {
		log.WithField("executableGroup", "ping").Error("One or more required executables were not found in $PATH.  Will still attempt to run, but this will probably fail")
	}
}
