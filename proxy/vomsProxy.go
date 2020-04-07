package proxy

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"text/template"

	log "github.com/sirupsen/logrus"

	"cdcvs.fnal.gov/discompsupp/ken_proxy_push/v3/utils"
)

const (
	vomsProxyInitArgs = "-dont-verify-ac -rfc -valid 24:00 -voms {{.VomsFQAN}} -cert {{.CertFile}} -key {{.KeyFile}} -out {{.OutfilePath}}"
	vomsProxyInfoArgs = "-fqan -file {{.ProxyPath}}"
)

var (
	vomsProxyExecutables = map[string]string{
		"voms-proxy-init": "",
		"voms-proxy-info": "",
	}
	vomsProxyInitTemplate = template.Must(template.New("voms-proxy-init").Parse(vomsProxyInitArgs))
	vomsProxyInfoTemplate = template.Must(template.New("voms-proxy-info").Parse(vomsProxyInfoArgs))
)

// getVomsProxyer encapsulates the method to obtain a VOMS proxy from an object
type getVomsProxyer interface {
	getVomsProxy(ctx context.Context, vomsFQAN string) (*VomsProxy, error)
}

// VomsProxy contains the information generally needed from a VOMS proxy, along with the Cert object used to create the VomsProxy itself
// Implements the getVomsProxyer, and copyProxyer interfaces
type VomsProxy struct {
	Path string
	Role string
	DN   string
	Cert
}

// NewVomsProxy returns a VOMS proxy and a teardown func from a getVomsProxyer
func NewVomsProxy(ctx context.Context, vp getVomsProxyer, vomsFQAN string) (*VomsProxy, func() error, error) {
	teardown := func() error { return nil }

	v, err := vp.getVomsProxy(ctx, vomsFQAN)
	if err != nil {
		err := &NewVomsProxyError{"Could not generate a VOMS proxy"}
		log.WithFields(log.Fields{
			"vomsProxyer": fmt.Sprintf("%v", vp),
			"FQAN":        vomsFQAN,
		}).Error(err)
		log.WithFields(log.Fields{
			"vomsProxyer": fmt.Sprintf("%v", vp),
			"FQAN":        vomsFQAN,
		}).Debug("Attempting to clean up VOMS Proxy file")
		if err2 := v.Remove(); err2 != nil && !os.IsNotExist(err2) {
			log.WithFields(log.Fields{
				"vomsProxyer": fmt.Sprintf("%v", vp),
				"FQAN":        vomsFQAN,
			}).Error("Cleanup failed")
		}
		return &VomsProxy{}, teardown, err
	}

	teardown = func() error { return v.Remove() }

	log.WithFields(log.Fields{
		"Path": v.Path,
		"Role": v.Role,
		"DN":   v.DN,
	}).Debug("Generated VOMS Proxy successfully")
	return v, teardown, nil
}

// NewVomsProxyError indicates that a new VomsProxy could not be created from a getVomsProxyer
type NewVomsProxyError struct{ message string }

func (n *NewVomsProxyError) Error() string { return n.message }

// Check runs voms-proxy-info to make sure that voms-proxy-init didn't lie to us...which it does way too often
func (v *VomsProxy) Check(ctx context.Context) error {
	var b strings.Builder

	cArgs := struct{ ProxyPath string }{ProxyPath: v.Path}

	if err := vomsProxyInfoTemplate.Execute(&b, cArgs); err != nil {
		err := &VomsProxyCheckError{fmt.Sprintf("Could not execute voms-proxy-info template: %s", err.Error())}
		log.WithField("proxyPath", v.Path).Error(err)
		return err
	}

	args, err := utils.GetArgsFromTemplate(b.String())
	if err != nil {
		err := &VomsProxyCheckError{fmt.Sprintf("Could not get voms-proxy-info command arguments from template: %s", err.Error())}
		log.WithField("proxyPath", v.Path).Error(err)
		return err
	}

	// Run voms-proxy-info -fqan
	cmd := exec.CommandContext(ctx, vomsProxyExecutables["voms-proxy-info"], args...)
	out, err := cmd.Output()
	if err != nil {
		err := &VomsProxyCheckError{fmt.Sprintf("Could not execute voms-proxy-info command: %s", err.Error())}
		log.WithFields(log.Fields{
			"proxyPath": v.Path,
			"role":      v.Role,
			"command":   strings.Join(cmd.Args, " "),
		}).Error(err)
		return err
	}

	// Read top line from output of voms-proxy-info -fqan
	fqanReader := bufio.NewReader(bytes.NewReader(out))
	topLine, err := fqanReader.ReadString('\n')
	if err != nil {
		err := &VomsProxyCheckError{fmt.Sprintf("Could not read lines from voms-proxy-info command output: %s", err.Error())}
		log.WithFields(log.Fields{
			"proxyPath": v.Path,
			"role":      v.Role,
		}).Error(err)
		return err
	}
	topLine = strings.TrimSpace(topLine)

	// Check the top line against our desired FQAN
	testRole := getRoleFromFQAN(topLine)
	if testRole != v.Role {
		err := &VomsProxyCheckError{"VOMS Proxy validation failed: voms-proxy-info -fqan disagrees with nominal role of VOMS proxy object"}
		log.WithFields(log.Fields{
			"proxyPath": v.Path,
			"role":      v.Role,
			"testRole":  testRole,
		}).Error(err)
		return err
	}

	return nil
}

// VomsProxyCheckError indicates that the check on a VomsProxy failed
type VomsProxyCheckError struct{ message string }

func (v *VomsProxyCheckError) Error() string { return v.message }

// Remove deletes the file at VomsProxy.Path
func (v *VomsProxy) Remove() error {
	if err := os.Remove(v.Path); os.IsNotExist(err) {
		log.WithField("path", v.Path).Error("VOMS Proxy file does not exist")
		return os.ErrNotExist
	} else if err != nil {
		log.WithField("path", v.Path).Error(err)
		return err
	}
	log.WithField("path", v.Path).Debug("VOMS Proxy removed")
	return nil
}

// getVomsProxy obtains a VOMS proxy from a ServiceCert by running voms-proxy-init
func (s *ServiceCert) getVomsProxy(ctx context.Context, vomsFQAN string) (*VomsProxy, error) {
	var b strings.Builder

	_outfile, err := ioutil.TempFile("", "managed_proxy_voms_")
	if err != nil {
		err := fmt.Sprintf("Couldn't get tempfile: %s", err.Error())
		log.WithField("certPath", s.certPath).Error(err)
		return &VomsProxy{}, errors.New(err)
	}
	outfile := _outfile.Name()
	v := VomsProxy{Path: outfile, Role: getRoleFromFQAN(vomsFQAN), Cert: s}

	cArgs := struct{ VomsFQAN, CertFile, KeyFile, OutfilePath string }{
		VomsFQAN:    vomsFQAN,
		CertFile:    s.certPath,
		KeyFile:     s.keyPath,
		OutfilePath: outfile,
	}

	if err := vomsProxyInitTemplate.Execute(&b, cArgs); err != nil {
		err := fmt.Sprintf("Could not execute voms-proxy-init template: %s", err.Error())
		log.WithField("certPath", s.certPath).Error(err)
		return &v, errors.New(err)
	}

	args, err := utils.GetArgsFromTemplate(b.String())
	if err != nil {
		err := fmt.Sprintf("Could not get voms-proxy-init command arguments from template: %s", err.Error())
		log.WithField("certPath", s.certPath).Error(err)
		return &v, errors.New(err)
	}

	cmd := exec.CommandContext(ctx, vomsProxyExecutables["voms-proxy-init"], args...)
	if err := cmd.Run(); err != nil {
		err := fmt.Sprintf("Could not execute voms-proxy-init command: %s", err.Error())
		log.WithFields(log.Fields{
			"certPath": s.certPath,
			"vomsFQAN": vomsFQAN,
			"command":  strings.Join(cmd.Args, " "),
		}).Error(err)
		return &v, errors.New(err)
	}

	v.DN = v.Subject()
	log.WithFields(log.Fields{
		"path":    v.Path,
		"subject": v.DN,
		"role":    v.Role,
	}).Debug("Successfully generated VOMS proxy")

	return &v, nil
}

func init() {
	if err := utils.CheckForExecutables(vomsProxyExecutables); err != nil {
		log.WithField("executableGroup", "vomsProxy").Error("One or more required executables were not found in $PATH.  Will still attempt to run, but this will probably fail")
	}
}

// getRoleFromFQAN parses the fqan string and returns the role
func getRoleFromFQAN(fqan string) string {
	pattern := regexp.MustCompile("^.+Role=([a-zA-Z]+)(/Capability=NULL)?$")
	return pattern.FindStringSubmatch(fqan)[1]
}
