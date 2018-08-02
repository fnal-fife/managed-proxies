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

	log "github.com/sirupsen/logrus"
)

// getProxyer is an interface that wraps up the getProxy method.  It is meant to be used in methods that obtain a VOMS proxy
type getProxyer interface {
	getProxy(context.Context, VPIConfig) (string, error)
}

// vomsProxy stores the information needed to uniquely identify the elements of a VOMS proxy.
type vomsProxy struct {
	fqan     string
	role     string
	account  string
	certfile string
	keyfile  string
}

// vomsProxyInitStatus stores information about an attempt to run voms-proxy-init to generate a VOMS proxy.
// If there was an error, it's stored in err.
type vomsProxyInitStatus struct {
	filename string
	err      error
}

// getProxies launches goroutines that run getProxy to generate the appropriate proxies on the local machine.
// Calling getProxies will generate voms X509 proxies of all of the proxies passed in as getProxyer objects.  It returns
// a channel, on which it reports the status of each attempt
func getProxies(ctx context.Context, vConfig VPIConfig, proxies ...getProxyer) <-chan vomsProxyInitStatus {
	c := make(chan vomsProxyInitStatus, len(proxies))
	var wg sync.WaitGroup
	wg.Add(len(proxies))

	for _, v := range proxies {
		go func(v getProxyer) {
			defer wg.Done()
			outfile, err := v.getProxy(ctx, vConfig)
			vpi := vomsProxyInitStatus{outfile, err}
			c <- vpi
		}(v)
	}

	// Wait for all goroutines to finish, then close channel so that expt Worker can proceed
	go func() {
		defer close(c)
		wg.Wait()
	}()

	return c
}

// createVomsProxyObjects takes the configuration information for an experiment and generates a slice of vomsProxy objects
// for that experiment
func createVomsProxyObjects(ctx context.Context, eConfig ExptConfig) (vSlice []getProxyer) {

	for account, role := range eConfig.Accounts {
		v := vomsProxy{account: account, role: role}
		v.fqan = eConfig.VomsPrefix + "Role=" + role

		if eConfig.CertFile != "" {
			v.certfile = eConfig.CertFile
		} else {
			v.certfile = path.Join(eConfig.CertBaseDir, account+".cert")
		}

		if eConfig.KeyFile != "" {
			v.keyfile = eConfig.KeyFile
		} else {
			v.keyfile = path.Join(eConfig.CertBaseDir, account+".key")
		}

		vSlice = append(vSlice, &v)
	}
	return
}

// getProxy receives a *vomsProxy object, and uses its properties to run voms-proxy-init to generate a VOMS Proxy.
// It returns the location of the generated proxy file and an error if the attempt fails.
func (v *vomsProxy) getProxy(ctx context.Context, vConfig VPIConfig) (string, error) {
	var vpiArgsTemplateOut bytes.Buffer

	outfile := v.account + "." + v.role + ".proxy"
	outfilePath := path.Join("proxies", outfile)

	var vpiMap = map[string]string{
		"VomsFQAN":    v.fqan,
		"CertFile":    v.certfile,
		"KeyFile":     v.keyfile,
		"OutfilePath": outfilePath,
	}

	// vpi.filename = outfile
	t := template.Must(template.New("vpiTemplate").Parse(vConfig["vpicommand"]))
	err := t.Execute(&vpiArgsTemplateOut, vpiMap)
	if err != nil {
		log.WithFields(log.Fields{
			"FQAN":     v.fqan,
			"account":  v.account,
			"certfile": v.certfile,
		}).Error(err)
		return outfile, err
	}

	vpiArgsString := vpiArgsTemplateOut.String()
	vpiArgs := strings.Fields(vpiArgsString)

	//	vpiargs := []string{"-rfc", "-valid", "24:00", "-voms",
	//		v.fqan, "-cert", v.certfile,
	//		"-key", v.keyfile, "-out", outfilePath}

	//cmd := exec.CommandContext(ctx, "/usr/bin/voms-proxy-init", vpiargs...)
	cmd := exec.CommandContext(ctx, vConfig["executable"], vpiArgs...)
	if cmdErr := cmd.Run(); cmdErr != nil {
		if e := ctx.Err(); e != nil {
			return "", e
		}
		err := fmt.Sprintf(`Error obtaining %s.  Please check the cert on 
				fifeutilgpvm01. \n%s Continuing on to next role.`, outfile, cmdErr)
		return "", errors.New(err)
	}
	return outfile, nil
}
