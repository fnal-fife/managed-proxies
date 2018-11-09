package proxy

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"strings"
	"text/template"
)

type VomsProxy struct {
	Path string
	FQAN string
	DN   string
}

const (
	vpiArgs = "-rfc -valid 24:00 -voms {{.VomsFQAN}} -cert {{.CertFile}} -key {{.KeyFile}} -out {{.OutfilePath}}"
)

var (
	vomsProxyExecutables = map[string]string{
		"voms-proxy-init": "",
		"scp":             "",
		"ssh":             "",
	}
	vpiTemplate = template.Must(template.New("voms-proxy-init").Parse(vpiArgs))
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
		fmt.Println("Couldn't get tempfile")
		return &VomsProxy{}, err
	}
	outfile := _outfile.Name()

	cArgs := struct{ VomsFQAN, CertFile, KeyFile, OutfilePath string }{
		VomsFQAN:    vomsFQAN,
		CertFile:    s.certPath,
		KeyFile:     s.keyPath,
		OutfilePath: outfile,
	}

	if err := vpiTemplate.Execute(&b, cArgs); err != nil {
		fmt.Println("Could not execute voms-proxy-init template.")
		return &VomsProxy{}, err
	}

	args, err := getArgsFromTemplate(b.String())
	if err != nil {
		fmt.Println("Could not get voms-proxy-init command arguments from template")
		return &VomsProxy{}, err
	}

	cmd := exec.CommandContext(ctx, vomsProxyExecutables["voms-proxy-init"], args...)
	if err := cmd.Run(); err != nil {
		fmt.Println("Could not execute voms-proxy-init command")
		//TODO
		fmt.Println(err)
		return &VomsProxy{}, err
	}

	v := VomsProxy{Path: outfile, FQAN: vomsFQAN}

	_dn, err := v.getDN(ctx)
	if err != nil {
		fmt.Println("Could not get proxy subject from voms proxy")
		fmt.Println(err)
		return &VomsProxy{}, err
	}
	v.DN = _dn
	return &v, nil
}

func (v *VomsProxy) Remove() error {

	err := os.Remove(v.Path)

	if os.IsNotExist(err) {
		fmt.Println("VOMS Proxy file does not exist")
	} else if err != nil {
		fmt.Println(err)
	}

	return err
}

func (v *VomsProxy) getDN(ctx context.Context) (string, error) {
	dn, err := getCertSubject(ctx, v.Path)
	if err != nil {
		fmt.Println(err)
		return "", err
	}
	return dn, nil
}

type proxyTransferer interface {
	copyProxy(node, account, destdir string) error
	chmodProxy(node, account, destdir string) error
}

func (v *VomsProxy) copyProxy(node, account, destdir string) error {
	return nil
}

func (v *VomsProxy) chmodProxy(node, account, destdir string) error {
	return nil
}

// Both of these should be called from an interface rather than the actual object.  Then we can unit test more easily i.e. pushProxyer.copyProxy(....)

func init() { checkForExecutables(vomsProxyExecutables) }
