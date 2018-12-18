package proxy

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"strings"
	"text/template"

	"cdcvs.fnal.gov/discompsupp/ken_proxy_push/utils"
)

type VomsProxy struct {
	Path string
	FQAN string
	DN   string
}

const (
	vpiArgs   = "-rfc -valid 24:00 -voms {{.VomsFQAN}} -cert {{.CertFile}} -key {{.KeyFile}} -out {{.OutfilePath}}"
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

type vomsProxyer interface {
	getVomsProxy(ctx context.Context, vomsFQAN string) (*VomsProxy, error)
}

func NewVOMSProxy(ctx context.Context, vp vomsProxyer, vomsFQAN string) (*VomsProxy, error) {
	v, err := vp.getVomsProxy(ctx, vomsFQAN)
	if err != nil {
		return &VomsProxy{}, err
	}
	return v, nil
}

func (s *serviceCert) getVomsProxy(ctx context.Context, vomsFQAN string) (*VomsProxy, error) {
	var b strings.Builder

	_outfile, err := ioutil.TempFile("", "managed_proxy_voms_")
	if err != nil {
		return &VomsProxy{}, fmt.Errorf("Couldn't get tempfile, %s", err.Error())
	}
	outfile := _outfile.Name()

	cArgs := struct{ VomsFQAN, CertFile, KeyFile, OutfilePath string }{
		VomsFQAN:    vomsFQAN,
		CertFile:    s.certPath,
		KeyFile:     s.keyPath,
		OutfilePath: outfile,
	}

	if err := vpiTemplate.Execute(&b, cArgs); err != nil {
		return &VomsProxy{}, fmt.Errorf("Could not execute voms-proxy-init template: %s", err.Error())
	}

	args, err := utils.GetArgsFromTemplate(b.String())
	if err != nil {
		return &VomsProxy{}, fmt.Errorf("Could not get voms-proxy-init command arguments from template: %s", err.Error())
	}

	cmd := exec.CommandContext(ctx, vomsProxyExecutables["voms-proxy-init"], args...)
	if err := cmd.Run(); err != nil {
		return &VomsProxy{}, fmt.Errorf("Could not execute voms-proxy-init command: %s", err.Error())
	}

	v := VomsProxy{Path: outfile, FQAN: vomsFQAN}

	_dn, err := s.getCertSubject(ctx)
	if err != nil {
		return &VomsProxy{}, fmt.Errorf("Could not get proxy subject from voms proxy: %s", err.Error())
	}
	v.DN = _dn
	return &v, nil
}

func (v *VomsProxy) Remove() error {
	if err := os.Remove(v.Path); os.IsNotExist(err) {
		return errors.New("VOMS Proxy file does not exist")
	} else {
		return err
	}
}

type proxyTransferer interface {
	CopyProxy(ctx context.Context, node, account, dest string) error
}

func (v *VomsProxy) CopyProxy(ctx context.Context, node, account, dest string) error {
	return rsyncFile(ctx, v.Path, node, account, dest, sshOpts)
}

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
		return fmt.Errorf("Could not execute rsyncTemplate: %s", err.Error())
	}

	args, err := utils.GetArgsFromTemplate(b.String())
	if err != nil {
		return fmt.Errorf("Could not get rsync command arguments from template: %s", err.Error())
	}

	cmd := exec.CommandContext(ctx, vomsProxyExecutables["rsync"], args...)
	if out, err := cmd.CombinedOutput(); err != nil {
		return errors.New(fmt.Sprintf("%s\n%s", string(out), string(err.Error())))
	}
	return nil

}

// Both of these should be called from an interface rather than the actual object.  Then we can unit test more easily i.e. pushProxyer.copyProxy(....)

func init() {

	if err := utils.CheckForExecutables(vomsProxyExecutables); err != nil {
		fmt.Printf("Note that one or more required executables were not found in $PATH: %s\n", err)
	}
}
