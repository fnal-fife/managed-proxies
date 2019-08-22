package experiment

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path"
	"strings"
	"sync"
	//	"text/tabwriter"
	"time"

	log "github.com/sirupsen/logrus"

	"cdcvs.fnal.gov/discompsupp/ken_proxy_push/v3/proxy"
	"cdcvs.fnal.gov/discompsupp/ken_proxy_push/v3/utils"
)

// vomsProxyInitStatus stores information about an attempt to run voms-proxy-init to generate a VOMS proxy.
// If there was an error, it's stored in err.
type vomsProxyInitStatus struct {
	vomsProxy *proxy.VomsProxy
	err       error
}

// getVomsProxyersForExperiment takes the service cert and list of accounts/roles, and creates a slice of proxy.VomsProxyer objects for that experiment
func getVomsProxyersForExperiment(ctx context.Context, certBaseDir, configCertPath, configKeyPath string, accounts map[string]string) (map[string]proxy.VomsProxyer, error) {
	m := make(map[string]proxy.VomsProxyer, len(accounts))
	var e error
	var once sync.Once

	for account, role := range accounts {
		certPath, keyPath := setCertKeyLocation(certBaseDir, configCertPath, configKeyPath, account)
		certDuration, _ := time.ParseDuration("1m")
		certCtx, certCancel := context.WithTimeout(ctx, certDuration)

		cert, err := proxy.NewServiceCert(certCtx, certPath, keyPath)
		if err != nil {
			log.WithFields(log.Fields{
				"account":  account,
				"certPath": certPath,
				"keyPath":  keyPath,
			}).Error("Could not ingest service cert")
			e = err
			once.Do(certCancel)
			continue
		}

		m[role] = cert
		once.Do(certCancel)
	}
	return m, e
}

// setCertKeyLocation takes the configured certPath, keyPath (or lack thereof), and certBaseDir, and sets the paths to look for the service certificate and key
func setCertKeyLocation(certBaseDir, certPath, keyPath, account string) (string, string) {
	var certfile, keyfile string
	if certPath != "" {
		certfile = certPath
	} else {
		certfile = path.Join(certBaseDir, account+".cert")
	}

	if keyPath != "" {
		keyfile = keyPath
	} else {
		keyfile = path.Join(certBaseDir, account+".key")
	}

	return certfile, keyfile
}

// getVomsProxiesForExperiment takes a map of form role:proxyVomsProxyer, and generates voms proxies for each entry in the map.  It returns a channel that its caller should listen on
func getVomsProxiesForExperiment(ctx context.Context, vpMap map[string]proxy.VomsProxyer, vomsFQANPrefix string) <-chan vomsProxyInitStatus {
	c := make(chan vomsProxyInitStatus)
	var wg sync.WaitGroup

	wg.Add(len(vpMap))

	// Run vpi on each acct, role
	for role, vp := range vpMap {
		go func(role string, vp proxy.VomsProxyer) {
			defer wg.Done()
			fqan := vomsFQANPrefix + "Role=" + role
			v, err := proxy.NewVomsProxy(ctx, vp, fqan)
			checkErr := v.Check(ctx)
			if checkErr != nil {
				err = checkErr
			}
			vpi := vomsProxyInitStatus{v, err}
			c <- vpi
		}(role, vp)
	}

	// Wait for all goroutines to finish, then close channel so that caller knows there's no more objects to send in the channel
	go func() {
		defer close(c)
		wg.Wait()
	}()

	return c

}

// copyFileConfig holds the information needed to copy a proxy.Transferer to a destination node
type copyFileConfig struct {
	node, account, destPath, role string
	pt                            proxy.Transferer
}

// copyProxiesStatus stores information that uniquely identifies a VOMS proxy within an experiment (account, role) and
// the node to which it was attempted to be copied.  If there was an error doing so, it's stored in err.
type copyProxiesStatus struct {
	account, node, role string
	err                 error
}

// createCopyFileConfigs creates a slice of copyFileConfig objects for the experiment
func createCopyFileConfigs(vp []*proxy.VomsProxy, accountMap map[string]string, nodes []string, destDir string) []copyFileConfig {
	c := make([]copyFileConfig, 0)
	invertAcct := make(map[string]string) // accountMap is account:role; we want role:account
	for acct, role := range accountMap {
		invertAcct[role] = acct
	}

	for _, v := range vp {
		acct := invertAcct[v.Role]
		proxyFileName := acct + "." + v.Role + ".proxy"
		finalProxyPath := path.Join(destDir, acct, proxyFileName)

		for _, n := range nodes {
			_c := copyFileConfig{
				account:  acct,
				node:     n,
				destPath: finalProxyPath,
				role:     v.Role,
				pt:       v,
			}
			c = append(c, _c)
		}
	}
	return c
}

// copyAllProxies runs the CopyProxy method for each copyConfig.pt object.  It returns a channel its caller can receive the status on
func copyAllProxies(ctx context.Context, copyConfigs []copyFileConfig) <-chan copyProxiesStatus {
	numSlots := len(copyConfigs)

	c := make(chan copyProxiesStatus, numSlots)
	var wg sync.WaitGroup

	wg.Add(numSlots)

	for _, cp := range copyConfigs {
		go func(cp copyFileConfig) {
			cps := cp.createCopyProxiesStatus()
			defer wg.Done()

			cps.err = cp.pt.CopyProxy(ctx, cp.node, cp.account, cp.destPath)
			if cps.err != nil {
				if e := ctx.Err(); e == nil {
					log.WithFields(log.Fields{
						"node":            cp.node,
						"account":         cp.account,
						"role":            cp.role,
						"destinationPath": cp.destPath,
					}).Error("Could not copy proxy to destination")
				}
			}
			c <- cps
		}(cp)
	}

	// Wait for all goroutines to finish, then close channel so that caller knows there's no more objects to send in the channel
	go func() {
		defer close(c)
		wg.Wait()
	}()

	return c

}

// createCopyProxiesStatus is a helper function that creates a copyProxiesStatus object for later modification
func (c *copyFileConfig) createCopyProxiesStatus() copyProxiesStatus {
	return copyProxiesStatus{
		account: c.account,
		node:    c.node,
		role:    c.role,
		err:     nil,
	}
}

// getKerbTicket runs kinit to get a kerberos ticket
func getKerbTicket(ctx context.Context, krbConfig KerbConfig) error {
	var kinitExecutable = kinitExecutable // Local kinitExecutable = global kinitExecutablej

	// If we can't find the kinit executable there, then try to find it in $PATH
	if _, err := os.Stat(kinitExecutable); os.IsNotExist(err) {
		_kinitExecutable, _err := exec.LookPath("kinit")
		if _err != nil {
			log.WithField("caller", "getKerbTicket").Error("Could not find kinit executable in $PATH")
			return _err
		}
		log.Infof("Using kinit executable at %s", _kinitExecutable)
		kinitExecutable = _kinitExecutable
	}

	os.Setenv("KRB5CCNAME", krbConfig["krb5ccname"])
	kerbcmdargs := strings.Fields(krbConfig["kinitargs"])

	cmd := exec.CommandContext(ctx, kinitExecutable, kerbcmdargs...)
	if out, err := cmd.CombinedOutput(); err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			log.WithField("caller", "getKerbTicket").Error("Context timeout")
			return ctx.Err()
		}
		fmt.Println(string(out))
		log.Error(string(out))
		return err
	}
	return nil
}

// checkKeys looks at the portion of the configuration passed in and makes sure the required keys are present
func checkKeys(ctx context.Context, eConfig ExptConfig) error {
	if e := ctx.Err(); e != nil {
		log.WithField("caller", "checkKeys").Error(e)
		return e
	}

	if len(eConfig.Nodes) == 0 || len(eConfig.Accounts) == 0 {
		log.WithField("caller", "checkKeys").Error(checkKeysErrorString)
		return errors.New(checkKeysErrorString)
	}
	return nil
}

// failedPrettifyRolesNodesMap formats a map of failed nodes and roles into node, role columns and appends a message onto the beginning
func failedPrettifyRolesNodesMap(roleNodesMap map[string]map[string]error) string {
	empty := true

	for _, nodeMap := range roleNodesMap {
		if len(nodeMap) > 0 {
			empty = false
			break
		}
	}

	if empty {
		return ""
	}

	table := utils.DoubleErrorMapToTable(roleNodesMap, []string{"Role", "Node", "Error"})

	finalTable := fmt.Sprintf("The following is a list of nodes on which all proxies were not refreshed, and the corresponding roles for those failed proxy refreshes:\n\n%s", table)
	return finalTable

}

// generateNewErrorStringForTable is meant to change an error string based on whether it is currently equal to the defaultError.  If the testError and defaultError match, this func will return an error with the text of errStringSlice.  If they don't, then this func will append the contents of errStringSlice onto the testError text, and return a new error with the combined string.  The separator formats how different error strings should be distinguished.   This func should only be used to concatenate error strings
func generateNewErrorStringForTable(defaultError, testError error, errStringSlice []string, separator string) error {
	var newErrStringSlice []string
	if testError == defaultError {
		newErrStringSlice = errStringSlice
	} else {
		newErrStringSlice = append([]string{testError.Error()}, errStringSlice...)
	}

	return errors.New(strings.Join(newErrStringSlice, separator))
}
