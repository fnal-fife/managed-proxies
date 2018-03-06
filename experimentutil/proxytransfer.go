package experimentutil

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"path"
	"sync"

	"github.com/spf13/viper"
)

// pushProxyer is an interface that wraps up the methods meant to be used in pushing a VOMS proxy to an experiment's node
type pushProxyer interface {
	copyProxy(context.Context, []string) error
	chmodProxy(context.Context, []string) error
	createCopyProxiesStatus() copyProxiesStatus
}

// proxyTransferInfo contains the information needed to describe where on the source host to find the proxy, and where on the destination host to push that proxy
type proxyTransferInfo struct {
	account           string
	role              string
	node              string
	nodeDown          bool
	proxyFileName     string
	proxyFilePathSrc  string
	proxyFileNameDest string
}

// copyProxiesStatus stores information that uniquely identifies a VOMS proxy within an experiment (account, role) and
// the node to which it was attempted to be copied.  If there was an error doing so, it's stored in err.
type copyProxiesStatus struct {
	node    string
	account string
	role    string
	err     error
}

// copyProxies copies the proxies from the local machine to the experiment nodes as specified by pushProxyer variadic and
// changes their permissions. It returns a channel on which it reports the status of those operations.  The copy and
// change permission operations share a context that dictates their deadline.
func copyProxies(ctx context.Context, proxyTransfers ...pushProxyer) <-chan copyProxiesStatus {
	numSlots := len(proxyTransfers)
	sshopts := []string{"-o", "ConnectTimeout=30",
		"-o", "ServerAliveInterval=30",
		"-o", "ServerAliveCountMax=1"}
	c := make(chan copyProxiesStatus, numSlots)
	var wg sync.WaitGroup

	wg.Add(numSlots)

	for _, p := range proxyTransfers {
		go func(p pushProxyer) {
			cps := p.createCopyProxiesStatus()
			defer wg.Done()

			if cps.err = p.copyProxy(ctx, sshopts); cps.err != nil {
				if e := ctx.Err(); e == nil {
					cps.err = errors.New(cps.err.Error())
				}
				c <- cps
				return
			}
			if cps.err = p.chmodProxy(ctx, sshopts); cps.err != nil {
				if e := ctx.Err(); e == nil {
					cps.err = errors.New(cps.err.Error())
				}
			}
			c <- cps
		}(p)
	}

	// Wait for all goroutines to finish, then close channel so that expt Worker can proceed
	go func() {
		defer close(c)
		wg.Wait()
	}()

	return c
}

// createProxyTransferInfoObjects uses the configuration information to create a string of pushProxyer objects containing source and
// destination information for VOMS proxies
func createProxyTransferInfoObjects(ctx context.Context, exptConfig *viper.Viper, badNodesSlice []string) (p []pushProxyer) {
	badNodesMap := make(map[string]struct{})

	for _, node := range badNodesSlice {
		badNodesMap[node] = struct{}{}
	}

	for acct, role := range exptConfig.GetStringMapString("accounts") {
		// Create the proxy transfer objects, attach to slice
		proxyFile := acct + "." + role + ".proxy"
		proxyFilePath := path.Join("proxies", proxyFile)
		finalProxyPath := path.Join(exptConfig.GetString("dir"), acct, proxyFile)

		for _, node := range exptConfig.GetStringSlice("nodes") {
			var badnode bool
			if _, ok := badNodesMap[node]; ok {
				badnode = true
			}
			pInfo := proxyTransferInfo{
				account:           acct,
				role:              role,
				node:              node,
				nodeDown:          badnode,
				proxyFileName:     proxyFile,
				proxyFilePathSrc:  proxyFilePath,
				proxyFileNameDest: finalProxyPath}

			p = append(p, &pInfo)
		}
	}
	return
}

// copyProxy uses scp to copy the proxy to the destination node, putting it in a file specified by pt.proxyFileNameDest
// with ".new" appended.  It returns an error.
func (pt *proxyTransferInfo) copyProxy(ctx context.Context, sshopts []string) error {
	newProxyPath := pt.proxyFileNameDest + ".new"
	accountNode := pt.account + "@" + pt.node + ".fnal.gov"
	scpargs := append(sshopts, pt.proxyFilePathSrc, accountNode+":"+newProxyPath)
	scpCmd := exec.CommandContext(ctx, "scp", scpargs...)

	if cmdOut, cmdErr := scpCmd.CombinedOutput(); cmdErr != nil {
		if e := ctx.Err(); e != nil {
			return e
		}
		e := fmt.Sprintf("Copying proxy %s to node %s failed.  The error was %s: %s %s",
			pt.proxyFileName, pt.node, cmdErr, cmdOut, pt.generateBadNodeMsg())
		return errors.New(e)
	}
	return nil
}

// chmodProxy uses ssh and chmod to change the permissions of the proxy on the destination node, putting it in a file
// specified by pt.proxyFileNameDest.  It returns an error.
func (pt *proxyTransferInfo) chmodProxy(ctx context.Context, sshopts []string) error {
	newProxyPath := pt.proxyFileNameDest + ".new"
	accountNode := pt.account + "@" + pt.node + ".fnal.gov"
	sshargs := append(sshopts, accountNode,
		"chmod 400 "+newProxyPath+" ; mv -f "+newProxyPath+" "+pt.proxyFileNameDest)
	sshCmd := exec.CommandContext(ctx, "ssh", sshargs...)

	if cmdOut, cmdErr := sshCmd.CombinedOutput(); cmdErr != nil {
		if e := ctx.Err(); e != nil {
			return e
		}
		return fmt.Errorf("Error changing permission of proxy %s to mode 400 on %s. "+
			"The error was %s: %s. %s", pt.proxyFileName, pt.node, cmdErr, cmdOut, pt.generateBadNodeMsg())
	}
	return nil
}

// createCopyProxiesStatus creates a copyProxiesStatus object from a *proxyTransferInfo object
func (pt *proxyTransferInfo) createCopyProxiesStatus() copyProxiesStatus {
	return copyProxiesStatus{pt.node, pt.account, pt.role, nil}
}

// generateBadNodeMsg returns the appropriate notification of a bad node to be appended to errors for a proxyTransferInfo object
func (pt *proxyTransferInfo) generateBadNodeMsg() (msg string) {
	if pt.nodeDown {
		msg = "\n" + fmt.Sprintf("Node %s didn't respond to pings earlier - "+
			"so it's expected that copying there would fail. "+
			"It may be necessary for the experiment to request via a "+
			"ServiceNow ticket that the Scientific Server Infrastructure "+
			"group reboot the node.", pt.node)
	}
	return
}
