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

// getProxyer is an interface that wraps up the getProxy method.  It is meant to be used in methods that obtain a VOMS proxy
type getProxyer interface {
	getProxy(context.Context) (string, error)
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

// createVomsProxyObjects takes the configuration information for an experiment and generates a slice of vomsProxy objects
// for that experiment
func createVomsProxyObjects(ctx context.Context, exptConfig *viper.Viper, globalConfig map[string]string,
	exptname string) (vSlice []getProxyer) {
	var vomsprefix string

	if exptConfig.IsSet("vomsgroup") {
		vomsprefix = exptConfig.GetString("vomsgroup")
	} else {
		vomsprefix = "fermilab:/fermilab/" + exptname + "/"
	}

	for account, role := range exptConfig.GetStringMapString("accounts") {
		v := vomsProxy{account: account, role: role}
		v.fqan = vomsprefix + "Role=" + role

		if exptConfig.IsSet("certfile") {
			v.certfile = exptConfig.GetString("certfile")
		} else {
			v.certfile = path.Join(globalConfig["cert_base_dir"], account+".cert")
		}

		if exptConfig.IsSet("keyfile") {
			v.keyfile = exptConfig.GetString("keyfile")
		} else {
			v.keyfile = path.Join(globalConfig["cert_base_dir"], account+".key")
		}

		vSlice = append(vSlice, &v)
	}
	return
}

// getProxies launches goroutines that run getProxy to generate the appropriate proxies on the local machine.
// Calling getProxies will generate voms X509 proxies of all of the proxies passed in as getProxyer objects.  It returns
// a channel, on which it reports the status of each attempt
func getProxies(ctx context.Context, proxies ...getProxyer) <-chan vomsProxyInitStatus {
	c := make(chan vomsProxyInitStatus, len(proxies))
	var wg sync.WaitGroup
	wg.Add(len(proxies))

	for _, v := range proxies {
		go func(v getProxyer) {
			defer wg.Done()
			outfile, err := v.getProxy(ctx)
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

// getProxy receives a *vomsProxy object, and uses its properties to run voms-proxy-init to generate a VOMS Proxy.
// It returns the location of the generated proxy file and an error if the attempt fails.
func (v *vomsProxy) getProxy(ctx context.Context) (string, error) {
	outfile := v.account + "." + v.role + ".proxy"
	outfilePath := path.Join("proxies", outfile)

	// vpi.filename = outfile
	vpiargs := []string{"-rfc", "-valid", "24:00", "-voms",
		v.fqan, "-cert", v.certfile,
		"-key", v.keyfile, "-out", outfilePath}

	cmd := exec.CommandContext(ctx, "/usr/bin/voms-proxy-init", vpiargs...)
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
