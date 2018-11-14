package proxy

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"regexp"
	"strings"
	"text/template"
)

var (
	serviceCertExecutables = map[string]string{
		"openssl": "",
	}
	opensslArgs         = "x509 -noout -subject -in {{.CertPath}}"
	opensslTemplate     = template.Must(template.New("openssl").Parse(opensslArgs))
	DNFromSubjectRegexp = regexp.MustCompile("^subject=(.+)$")
)

type Cert interface {
	getDN(context.Context) (string, error)
	getCertPath() string
	getKeyPath() string
}

type serviceCert struct {
	certPath string
	keyPath  string
	DN       string
}

func (s *serviceCert) getDN(ctx context.Context) (string, error) {
	dn, err := getCertSubject(ctx, s.certPath)
	if err != nil {
		return "", err
	}
	return dn, nil
}

func (s *serviceCert) getCertPath() string { return s.certPath }
func (s *serviceCert) getKeyPath() string  { return s.keyPath }

func NewServiceCert(ctx context.Context, certPath, keyPath string) (*serviceCert, error) {
	dn, err := getCertSubject(ctx, certPath)
	if err != nil {
		err := fmt.Errorf("Could not get DN for cert: %s", err.Error())
		return &serviceCert{}, err
	}
	return &serviceCert{certPath, keyPath, dn}, nil
}

func getCertSubject(ctx context.Context, certPath string) (string, error) {
	var b strings.Builder

	cArgs := struct{ CertPath string }{
		CertPath: certPath,
	}

	if err := opensslTemplate.Execute(&b, cArgs); err != nil {
		err := fmt.Errorf("Could not execute openssl template: %s.", err.Error())
		return "", err
	}

	args, err := getArgsFromTemplate(b.String())
	if err != nil {
		err := fmt.Errorf("Could not get openssl command arguments from template: %s", err.Error())
		return "", err
	}

	cmd := exec.CommandContext(ctx, serviceCertExecutables["openssl"], args...)
	out, err := cmd.Output()
	if err != nil {
		err := fmt.Errorf("Could not execute openssl command: %s.", err.Error())
		return "", err
	}

	processedOut := strings.TrimSpace(string(out))

	DNMatches := DNFromSubjectRegexp.FindAllStringSubmatch(processedOut, -1)
	if len(DNMatches) != 1 {
		return "", errors.New("Either not enough or too many subject strings in output of openssl")
	}
	DN := strings.TrimSpace(DNMatches[0][1])
	return DN, nil

}

func init() { checkForExecutables(serviceCertExecutables) }
