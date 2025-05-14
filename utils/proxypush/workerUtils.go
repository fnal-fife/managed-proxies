package proxypush

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/fnal-fife/managed-proxies/v5/proxy"
	"github.com/fnal-fife/managed-proxies/v5/utils"
)

// Functions and types are roughly presented in the order they'll be called during a proxy push

// getKerbTicket runs kinit to get a kerberos ticket
func getKerbTicket(ctx context.Context, krbConfig utils.KerbConfig) error {
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

// vomsProxyInitStatus stores information about an attempt to run voms-proxy-init to generate a VOMS proxy.
// If there was an error, it's stored in err.
type vomsProxyInitStatus struct {
	vomsProxy    *vomsProxy
	teardownFunc func() error
	err          error
}

type certKeyPair struct{ certPath, keyPath string }

// getVomsProxyersForExperiment takes a map of roles and *certKeyPairs, and creates a map of role: proxy.GetVomsProxyer objects for that experiment
func ingestServiceCertsForExperiment(ctx context.Context, certMap map[string]*certKeyPair) (map[string]*proxy.ServiceCert, error) {
	m := make(map[string]*proxy.ServiceCert, len(certMap))
	var e error
	var once sync.Once

	for role, c := range certMap {
		certDuration, _ := time.ParseDuration("1m")
		certCtx, certCancel := context.WithTimeout(ctx, certDuration)
		cert, err := proxy.NewServiceCert(certCtx, c.certPath, c.keyPath)
		if err != nil {
			log.WithFields(log.Fields{
				"role":     role,
				"certPath": c.certPath,
				"keyPath":  c.keyPath,
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

// getVomsProxiesForExperiment takes a map of form role:proxyVomsProxyer, and generates voms proxies for each entry in the map.  It returns a channel that its caller should listen on
func generateVomsProxiesForExperiment(ctx context.Context, certMap map[string]*proxy.ServiceCert, vomsFQANPrefix string) <-chan vomsProxyInitStatus {
	c := make(chan vomsProxyInitStatus)
	var wg sync.WaitGroup

	wg.Add(len(certMap))

	// Run vpi on each acct, role
	for role, cert := range certMap {
		go func(role string, cert *proxy.ServiceCert) {
			defer wg.Done()
			fqan := vomsFQANPrefix + "Role=" + role

			v, teardown, err := proxy.NewVomsProxy(ctx, cert, fqan)
			if err != nil {
				certi := vomsProxyInitStatus{newWrappedVomsProxy(v), teardown, err}
				c <- certi
				return
			}

			checkErr := v.Check(ctx)
			if checkErr == nil {
				log.WithFields(log.Fields{
					"Role": v.Role,
					"DN":   v.DN,
				}).Debug("Successfully checked new VOMS Proxy")
			}
			vpiStatus := vomsProxyInitStatus{newWrappedVomsProxy(v), teardown, checkErr}
			c <- vpiStatus

		}(role, cert)
	}

	// Wait for all goroutines to finish, then close channel so that caller knows there's no more objects to send in the channel
	go func() {
		defer close(c)
		wg.Wait()
	}()

	return c

}

// copyFileConfig holds the information needed to copy a vomsProxy to a destination node as specified in rsyncSetup
type copyFileConfig struct {
	*rsyncSetup
	*vomsProxy
}

// copyProxiesStatus stores information that uniquely identifies a VOMS proxy within an experiment (account, role) and
// the node to which it was attempted to be copied.  If there was an error doing so, it's stored in err.
type copyProxiesStatus struct {
	account, node, role string
	err                 error
}

// createCopyProxiesStatus is a helper function that creates a copyProxiesStatus object for later modification
func (c *copyFileConfig) createCopyProxiesStatus() copyProxiesStatus {
	return copyProxiesStatus{
		account: c.account,
		node:    c.node,
		role:    c.Role,
		err:     nil,
	}
}

// createCopyFileConfigs creates a slice of copyFileConfig objects for the experiment
func createCopyFileConfigs(vp []*vomsProxy, accountMap map[string]string, nodes []string, destDir string, sshOptsfromConfig string) []copyFileConfig {
	c := make([]copyFileConfig, 0)
	invertAcct := make(map[string]string) // accountMap is account:role; we want role:account
	for acct, role := range accountMap {
		invertAcct[role] = acct
	}

	var _sshOpts string
	if sshOptsfromConfig != "" {
		_sshOpts = sshOptsfromConfig
	} else {
		_sshOpts = sshOpts
	}

	for _, v := range vp {
		acct := invertAcct[v.Role]
		proxyFileName := acct + "." + v.Role + ".proxy"
		finalProxyPath := path.Join(destDir, acct, proxyFileName)

		// TODO For now, we're going to hardcode 3 retries, with a 30s sleep between them. This product will
		// soon be retired, so we're not going to spend a lot of time on this making it configurable
		for _, n := range nodes {
			_c := copyFileConfig{
				rsyncSetup: &rsyncSetup{
					account:     acct,
					node:        n,
					destination: finalProxyPath,
					sshOpts:     _sshOpts,
					numRetries:  3,
					retrySleep:  30 * time.Second,
				},
				vomsProxy: v,
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

			cps.err = cp.copyToNodeViaConfig(ctx)
			if cps.err != nil {
				if e := ctx.Err(); e == nil {
					log.WithFields(log.Fields{
						"node":            cp.node,
						"account":         cp.account,
						"role":            cp.Role,
						"destinationPath": cp.destination,
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

// Auxiliary functions

// getCertKeyPair takes the configured certPath, keyPath (or lack thereof), and certBaseDir, and sets the paths to look for the service certificate and key
func getCertKeyPair(certBaseDir, certPath, keyPath, account string) *certKeyPair {
	if certPath != "" && keyPath != "" {
		return &certKeyPair{certPath, keyPath}
	}
	return &certKeyPair{
		path.Join(certBaseDir, account+".cert"),
		path.Join(certBaseDir, account+".key"),
	}
}

// copyToNodeViaConfig copies a VOMS Proxy to a desination
func (c *copyFileConfig) copyToNodeViaConfig(ctx context.Context) error {
	return copySourceToDestination(ctx, c.vomsProxy, c.rsyncSetup)
}

// checkKeys looks at the portion of the configuration passed in and makes sure the required keys are present
func checkKeys(ctx context.Context, eConfig *utils.ExptConfig) error {
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
