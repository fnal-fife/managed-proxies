package proxy

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"text/template"
)

type serviceCert struct {
	certPath string
	keyPath  string
	DN       string
}

var (
	serviceCertExecutables = map[string]string{
		"openssl": "",
	}
	opensslArgs         = "x509 -noout -subject -in {{.CertPath}}"
	opensslTemplate     = template.Must(template.New("openssl").Parse(opensslArgs))
	DNFromSubjectRegexp = regexp.MustCompile("^subject=(.+)$")
)

func init() {
	// Make sure our required executables are in $PATH
	for exe := range serviceCertExecutables {
		if pth, err := exec.LookPath(exe); err != nil {
			fmt.Printf("%s was not found in $PATH.  Exiting", exe)
			os.Exit(1)
		} else {
			serviceCertExecutables[exe] = pth
		}
	}
}

func NewServiceCert(ctx context.Context, certPath, keyPath string) (*serviceCert, error) {
	fmt.Println("Ingesting service cert")
	dn, err := getCertSubject(ctx, certPath)
	if err != nil {
		fmt.Println("Could not get DN for cert")
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
		fmt.Println("Could not execute openssl template.")
		return "", err
	}

	args, err := getArgsFromTemplate(b.String())
	if err != nil {
		fmt.Println("Could not get myproxy-store command arguments from template")
		return "", err
	}

	cmd := exec.CommandContext(ctx, serviceCertExecutables["openssl"], args...)
	out, err := cmd.Output()
	if err != nil {
		fmt.Println("Could not execute openssl command.")
		fmt.Println(err)
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
