package experimentutil

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"path"
	"strings"
	"sync"
	"text/template"
)

// pushProxyer is an interface that wraps up the methods meant to be used in pushing a VOMS proxy to an experiment's node
type pushProxyer interface {
	copyProxy(context.Context, []string, string) error
	chmodProxy(context.Context, []string, string) error
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
func copyProxies(ctx context.Context, sConfig SSHConfig, proxyTransfers ...pushProxyer) <-chan copyProxiesStatus {
	numSlots := len(proxyTransfers)
	//	sshopts := []string{"-o", "ConnectTimeout=30",
	//		"-o", "ServerAliveInterval=30",
	//		"-o", "ServerAliveCountMax=1"}

	sshOpts := strings.Fields(sConfig["sshopts"])

	c := make(chan copyProxiesStatus, numSlots)
	var wg sync.WaitGroup

	wg.Add(numSlots)

	for _, p := range proxyTransfers {
		go func(p pushProxyer) {
			cps := p.createCopyProxiesStatus()
			defer wg.Done()

			if cps.err = p.copyProxy(ctx, sshOpts, sConfig["scpargs"]); cps.err != nil {
				if e := ctx.Err(); e == nil {
					cps.err = errors.New(cps.err.Error())
				}
				c <- cps
				return
			}
			if cps.err = p.chmodProxy(ctx, sshOpts, sConfig["chmodargs"]); cps.err != nil {
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
func createProxyTransferInfoObjects(ctx context.Context, eConfig ExptConfig, badNodesSlice []string) (p []pushProxyer) {
	badNodesMap := make(map[string]struct{})

	for _, node := range badNodesSlice {
		badNodesMap[node] = struct{}{}
	}

	for acct, role := range eConfig.Accounts {
		// Create the proxy transfer objects, attach to slice
		proxyFile := acct + "." + role + ".proxy"
		proxyFilePath := path.Join("proxies", proxyFile)
		finalProxyPath := path.Join(eConfig.DestDir, acct, proxyFile)

		for _, node := range eConfig.Nodes {
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
func (pt *proxyTransferInfo) copyProxy(ctx context.Context, sshOpts []string, scpTemplate string) error {
	var scpArgsTemplateOut bytes.Buffer
	newProxyPath := pt.proxyFileNameDest + ".new"

	var scpMap = map[string]string{
		"ProxySourcePath": pt.proxyFilePathSrc,
		"Account":         pt.account,
		"Node":            pt.node,
		"NewProxyPath":    newProxyPath,
	}

	t := template.Must(template.New("scpTemplate").Parse(scpTemplate))
	err := t.Execute(&scpArgsTemplateOut, scpMap)
	if err != nil {
		return err
	}

	scpArgsString := scpArgsTemplateOut.String()
	scpArgs := sshOpts
	scpArgs = append(scpArgs, strings.Fields(scpArgsString)...)

	// accountNode := pt.account + "@" + pt.node + ".fnal.gov"

	//scpargs := append(sshopts, pt.proxyFilePathSrc, accountNode+":"+newProxyPath)

	scpExecutable, err := exec.LookPath("scp")
	if err != nil {
		return err
	}

	fmt.Println(scpExecutable, scpArgs)

	scpCmd := exec.CommandContext(ctx, scpExecutable, scpArgs...)

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
func (pt *proxyTransferInfo) chmodProxy(ctx context.Context, sshOpts []string, chmodTemplate string) error {
	var chmodArgsTemplateOut bytes.Buffer

	newProxyPath := pt.proxyFileNameDest + ".new"
	// accountNode := pt.account + "@" + pt.node + ".fnal.gov"
	var chmodMap = map[string]string{
		"Account":        pt.account,
		"Node":           pt.node,
		"NewProxyPath":   newProxyPath,
		"FinalProxyPath": pt.proxyFileNameDest,
	}

	t := template.Must(template.New("chmodTemplate").Parse(chmodTemplate))
	err := t.Execute(&chmodArgsTemplateOut, chmodMap)
	if err != nil {
		return err
	}

	chmodArgsString := chmodArgsTemplateOut.String()
	chmodArgs := sshOpts
	chmodArgs = append(chmodArgs, strings.Fields(chmodArgsString)...)

	//	sshargs := append(sshopts, accountNode,
	//"chmod 400 "+newProxyPath+" ; mv -f "+newProxyPath+" "+pt.proxyFileNameDest)
	sshExecutable, err := exec.LookPath("ssh")
	if err != nil {
		return err
	}

	chmodCmd := exec.CommandContext(ctx, sshExecutable, chmodArgs...)

	if cmdOut, cmdErr := chmodCmd.CombinedOutput(); cmdErr != nil {
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
