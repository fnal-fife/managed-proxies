package proxy

import (
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
	vpiArgs   = "-dont-verify-ac -rfc -valid 24:00 -voms {{.VomsFQAN}} -cert {{.CertFile}} -key {{.KeyFile}} -out {{.OutfilePath}}"
	sshOpts   = "-o ConnectTimeout=30 -o ServerAliveInterval=30 -o ServerAliveCountMax=1"
	rsyncArgs = "-p -e \"{{.SSHExe}} {{.SSHOpts}}\" --chmod=u=r,go= {{.SourcePath}} {{.Account}}@{{.Node}}.fnal.gov:{{.DestPath}}"
)

var (
	vomsProxyExecutables = map[string]string{
		"voms-proxy-init": "",
		"rsync":           "",
		"ssh":             "",
	}
	vpiTemplate   = template.Must(template.New("voms-proxy-init").Parse(vpiArgs))
	rsyncTemplate = template.Must(template.New("rsync").Parse(rsyncArgs))
)

// VomsProxyer encapsulates the method to obtain a VOMS proxy from an object
type VomsProxyer interface {
	getVomsProxy(ctx context.Context, vomsFQAN string) (*VomsProxy, error)
}

// ProxyTransferer encapsulates the method to copy an object to a destination node
type ProxyTransferer interface {
	CopyProxy(ctx context.Context, node, account, dest string) error
}

// VomsProxy contains the information generally needed from a VOMS proxy, along with the Cert object used to create the VomsProxy itself
// Implements the Cert, VomsProxyer, and ProxyTransferer interfaces
type VomsProxy struct {
	Path string
	Role string
	DN   string
	Cert
}

// NewVomsProxy returns a VOMS proxy from a VomsProxyer
func NewVomsProxy(ctx context.Context, vp VomsProxyer, vomsFQAN string) (*VomsProxy, error) {
	v, err := vp.getVomsProxy(ctx, vomsFQAN)
	if err != nil {
		err := "Could not generate a VOMS proxy"
		log.WithFields(log.Fields{
			"vomsProxyer": fmt.Sprintf("%v", vp),
			"FQAN":        vomsFQAN,
		}).Error(err)
		return &VomsProxy{}, errors.New(err)
	}
	log.WithFields(log.Fields{
		"Path": v.Path,
		"Role": v.Role,
		"DN":   v.DN,
	}).Debug("Generated VOMS Proxy successfully")
	return v, nil
}

// Remove deletes the file at VomsProxy.Path
func (v *VomsProxy) Remove() error {
	if err := os.Remove(v.Path); os.IsNotExist(err) {
		err := "VOMS Proxy file does not exist"
		log.WithField("path", v.Path).Error(err)
		return errors.New(err)
	} else if err != nil {
		log.WithField("path", v.Path).Error(err)
		return err
	}
	log.WithField("path", v.Path).Debug("VOMS Proxy removed")
	return nil
}

// CopyProxy copies the proxy from VomsProxy.Path to account@node:dest
func (v *VomsProxy) CopyProxy(ctx context.Context, node, account, dest string) error {
	err := rsyncFile(ctx, v.Path, node, account, dest, sshOpts)
	if err != nil {
		log.WithFields(log.Fields{
			"sourcePath": v.Path,
			"destPath":   dest,
			"node":       node,
			"account":    account,
		}).Error("Could not copy VOMS proxy to destination node")
	}
	return err
}

// getVomsProxy obtains a VOMS proxy from a serviceCert by running voms-proxy-init
func (s *serviceCert) getVomsProxy(ctx context.Context, vomsFQAN string) (*VomsProxy, error) {
	var b strings.Builder

	_outfile, err := ioutil.TempFile("", "managed_proxy_voms_")
	if err != nil {
		err := fmt.Sprintf("Couldn't get tempfile: %s", err.Error())
		log.WithField("certPath", s.getCertPath()).Error(err)
		return &VomsProxy{}, errors.New(err)
	}
	outfile := _outfile.Name()

	cArgs := struct{ VomsFQAN, CertFile, KeyFile, OutfilePath string }{
		VomsFQAN:    vomsFQAN,
		CertFile:    s.certPath,
		KeyFile:     s.keyPath,
		OutfilePath: outfile,
	}

	if err := vpiTemplate.Execute(&b, cArgs); err != nil {
		err := fmt.Sprintf("Could not execute voms-proxy-init template: %s", err.Error())
		log.WithField("certPath", s.getCertPath()).Error(err)
		return &VomsProxy{}, errors.New(err)
	}

	args, err := utils.GetArgsFromTemplate(b.String())
	if err != nil {
		err := fmt.Sprintf("Could not get voms-proxy-init command arguments from template: %s", err.Error())
		log.WithField("certPath", s.getCertPath()).Error(err)
		return &VomsProxy{}, errors.New(err)
	}

	cmd := exec.CommandContext(ctx, vomsProxyExecutables["voms-proxy-init"], args...)
	if err := cmd.Run(); err != nil {
		err := fmt.Sprintf("Could not execute voms-proxy-init command: %s", err.Error())
		log.WithFields(log.Fields{
			"certPath": s.getCertPath(),
			"vomsFQAN": vomsFQAN,
			"command":  strings.Join(cmd.Args, " "),
		}).Error(err)
		return &VomsProxy{}, errors.New(err)
	}

	v := VomsProxy{Path: outfile, Role: getRoleFromFQAN(vomsFQAN)}

	_dn, err := s.getCertSubject(ctx)
	if err != nil {
		err := "Could not get proxy subject from voms proxy"
		log.WithFields(log.Fields{
			"certPath": s.getCertPath(),
			"vomsFQAN": vomsFQAN,
		}).Error(err)
		return &VomsProxy{}, errors.New(err)
	}
	v.DN = _dn
	v.Cert = s
	log.WithFields(log.Fields{
		"path":    v.Path,
		"subject": v.DN,
		"role":    v.Role,
	}).Debug("Successfully generated VOMS proxy")

	return &v, nil
}

// rsyncFile runs rsync on a file at source, and syncs it with the destination account@node:dest
func rsyncFile(ctx context.Context, source, node, account, dest string, sshOptions string) error {
	var b strings.Builder

	cArgs := struct{ SSHExe, SSHOpts, SourcePath, Account, Node, DestPath string }{
		SSHExe:     vomsProxyExecutables["ssh"],
		SSHOpts:    sshOptions,
		SourcePath: source,
		Account:    account,
		Node:       node,
		DestPath:   dest,
	}

	if err := rsyncTemplate.Execute(&b, cArgs); err != nil {
		err := fmt.Sprintf("Could not execute rsync template: %s", err.Error())
		log.WithField("source", source).Error(err)
		return errors.New(err)
	}

	args, err := utils.GetArgsFromTemplate(b.String())
	if err != nil {
		err := fmt.Sprintf("Could not get rsync command arguments from template: %s", err.Error())
		log.WithField("source", source).Error(err)
		return errors.New(err)
	}

	cmd := exec.CommandContext(ctx, vomsProxyExecutables["rsync"], args...)
	if err := cmd.Run(); err != nil {
		err := fmt.Sprintf("rsync command failed: %s", err.Error())
		log.WithFields(log.Fields{
			"sshOpts":    sshOptions,
			"sourcePath": source,
			"account":    account,
			"node":       node,
			"destPath":   dest,
			"command":    strings.Join(cmd.Args, " "),
		}).Error(err)

		return errors.New(err)
	}

	log.WithFields(log.Fields{
		"account":  account,
		"node":     node,
		"destPath": dest,
	}).Debug("rsync successful")
	return nil

}

func (v *VomsProxy) getCertPath() string { return v.Cert.getCertPath() }
func (v *VomsProxy) getKeyPath() string  { return v.Cert.getKeyPath() }

func (v *VomsProxy) getCertSubject(ctx context.Context) (string, error) {
	dn, err := v.Cert.getCertSubject(ctx)
	if err != nil {
		err := "Could not get subject for VOMS proxy"
		log.WithField("certPath", v.Cert.getCertPath()).Error(err)
		return "", errors.New(err)
	}
	return dn, nil
}

func init() {
	if err := utils.CheckForExecutables(vomsProxyExecutables); err != nil {
		log.WithField("executableGroup", "vomsProxy").Error("One or more required executables were not found in $PATH.  Will still attempt to run, but this will probably fail")
	}
}

// getRoleFromFQAN parses the fqan string and returns the role
func getRoleFromFQAN(fqan string) string {
	pattern := regexp.MustCompile("^.+Role=([a-zA-Z]+)$")
	return pattern.FindStringSubmatch(fqan)[1]
}
