package experimentutil

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path"
	"strings"
	"sync"
	"time"

	"cdcvs.fnal.gov/discompsupp/ken_proxy_push/proxy"
	log "github.com/sirupsen/logrus"
)

// vomsProxyInitStatus stores information about an attempt to run voms-proxy-init to generate a VOMS proxy.
// If there was an error, it's stored in err.
type vomsProxyInitStatus struct {
	vomsProxy *proxy.VomsProxy
	err       error
}

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

func getVomsProxiesForExperiment(ctx context.Context, vpMap map[string]proxy.VomsProxyer, vomsFQANPrefix string) <-chan vomsProxyInitStatus {
	c := make(chan vomsProxyInitStatus)
	var wg sync.WaitGroup

	// Run vpi on each acct, role
	wg.Add(len(vpMap))

	for role, vp := range vpMap {
		go func(role string, vp proxy.VomsProxyer) {
			defer wg.Done()
			fqan := vomsFQANPrefix + "Role=" + role
			v, err := proxy.NewVomsProxy(ctx, vp, fqan)
			vpi := vomsProxyInitStatus{v, err}
			c <- vpi
		}(role, vp)
	}

	// Wait for all goroutines to finish, then close channel so that expt Worker can proceed
	go func() {
		defer close(c)
		wg.Wait()
	}()

	return c

}

type copyFileConfig struct {
	node, account, destPath, role string
	pt                            proxy.ProxyTransferer
}

// copyProxiesStatus stores information that uniquely identifies a VOMS proxy within an experiment (account, role) and
// the node to which it was attempted to be copied.  If there was an error doing so, it's stored in err.
type copyProxiesStatus struct {
	account, node, role string
	err                 error
}

func createCopyFileConfigs(vp []*proxy.VomsProxy, accountMap map[string]string, nodes []string, destDir string) []copyFileConfig {
	c := make([]copyFileConfig, 0)
	invertAcct := make(map[string]string)
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

	// Wait for all goroutines to finish, then close channel so that expt Worker can proceed
	go func() {
		defer close(c)
		wg.Wait()
	}()

	return c

}

func (c *copyFileConfig) createCopyProxiesStatus() copyProxiesStatus {
	return copyProxiesStatus{c.node, c.account, c.role, nil}
}

// getKerbTicket runs kinit to get a kerberos ticket
func getKerbTicket(ctx context.Context, krbConfig KerbConfig) error {
	os.Setenv("KRB5CCNAME", krbConfig["krb5ccname"])
	kerbcmdargs := strings.Fields(krbConfig["kinitargs"])

	cmd := exec.CommandContext(ctx, kinitExecutable, kerbcmdargs...)
	if err := cmd.Run(); err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			log.WithField("caller", "getKerbTicket").Error("Context timeout")
			return ctx.Err()
		}
		log.Error(err)
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
		log.WithField("caller", "checkKeys").Error(checkKeysError)
		return errors.New(checkKeysError)
	}
	return nil
}
