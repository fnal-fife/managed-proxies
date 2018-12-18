package proxy

import (
	"context"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"io/ioutil"
	//	"regexp"
	"strings"
	//	"text/template"
)

//var (
//	serviceCertExecutables = map[string]string{
//		"openssl": "",
//	}
//	opensslArgs         = "x509 -noout -subject -in {{.CertPath}}"
//	opensslTemplate     = template.Must(template.New("openssl").Parse(opensslArgs))
//	DNFromSubjectRegexp = regexp.MustCompile("^subject=(.+)$")
//)

type Cert interface {
	//getDN(context.Context) (string, error)
	getCertSubject(context.Context) (string, error)
	getCertPath() string
	getKeyPath() string
}

type serviceCert struct {
	certPath string
	keyPath  string
	DN       string
}

//func (s *serviceCert) getDN(ctx context.Context) (string, error) {
//	if s.DN != "" {
//		return s.DN, nil
//	}
//	dn, err := getCertSubject(ctx, s.certPath)
//	if err != nil {
//		return "", err
//	}
//	return dn, nil
//}

func GetDN(ctx context.Context, c Cert) (string, error) {
	dn, err := c.getCertSubject(ctx)
	if err != nil {
		return "", err
	}
	return dn, err
}

func (s *serviceCert) getCertPath() string { return s.certPath }
func (s *serviceCert) getKeyPath() string  { return s.keyPath }
func (s *serviceCert) getCertSubject(ctx context.Context) (string, error) {
	if s.DN != "" {
		return s.DN, nil
	}

	certContent, err := ioutil.ReadFile(s.certPath)
	if err != nil {
		return "", fmt.Errorf("Could not read cert file at %s", s.certPath)
	}

	certDER, _ := pem.Decode(certContent)
	if certDER == nil {
		return "", errors.New("Could not decode PEM block containing cert")
	}

	cert, err := x509.ParseCertificate(certDER.Bytes)
	if err != nil {
		return "", errors.New("Could not parse certificate from DER data")
	}

	return parseDN(cert.Subject.Names, "/"), nil
}

func NewServiceCert(ctx context.Context, certPath, keyPath string) (*serviceCert, error) {
	s := &serviceCert{
		certPath: certPath,
		keyPath:  keyPath,
	}
	dn, err := s.getCertSubject(ctx)
	if err != nil {
		return nil, fmt.Errorf("Could not get DN for cert: %s", err.Error())
	}
	s.DN = dn
	return s, nil
}

//func getCertSubject(ctx context.Context, certPath string) (string, error) {
//	var b strings.Builder
//
//	cArgs := struct{ CertPath string }{
//		CertPath: certPath,
//	}
//
//	if err := opensslTemplate.Execute(&b, cArgs); err != nil {
//		return "", fmt.Errorf("Could not execute openssl template: %s.", err.Error())
//	}
//
//	args, err := getArgsFromTemplate(b.String())
//	if err != nil {
//		return "", fmt.Errorf("Could not get openssl command arguments from template: %s", err.Error())
//	}
//
//	cmd := exec.CommandContext(ctx, serviceCertExecutables["openssl"], args...)
//	out, err := cmd.Output()
//	if err != nil {
//		return "", fmt.Errorf("Could not execute openssl command: %s.", err.Error())
//	}
//
//	processedOut := strings.TrimSpace(string(out))
//
//	DNMatches := DNFromSubjectRegexp.FindAllStringSubmatch(processedOut, -1)
//	if len(DNMatches) != 1 {
//		return "", errors.New("Either not enough or too many subject strings in output of openssl")
//	}
//	DN := strings.TrimSpace(DNMatches[0][1])
//	return DN, nil
//
//}

// Thank you FERRY for this.  names can be *x509.Certificate.Subject.Names object
func parseDN(names []pkix.AttributeTypeAndValue, sep string) string {
	var oid = map[string]string{
		"2.5.4.3":                    "CN",
		"2.5.4.4":                    "SN",
		"2.5.4.5":                    "serialNumber",
		"2.5.4.6":                    "C",
		"2.5.4.7":                    "L",
		"2.5.4.8":                    "ST",
		"2.5.4.9":                    "streetAddress",
		"2.5.4.10":                   "O",
		"2.5.4.11":                   "OU",
		"2.5.4.12":                   "title",
		"2.5.4.17":                   "postalCode",
		"2.5.4.42":                   "GN",
		"2.5.4.43":                   "initials",
		"2.5.4.44":                   "generationQualifier",
		"2.5.4.46":                   "dnQualifier",
		"2.5.4.65":                   "pseudonym",
		"0.9.2342.19200300.100.1.25": "DC",
		"1.2.840.113549.1.9.1":       "emailAddress",
		"0.9.2342.19200300.100.1.1":  "userid",
	}
	var subject []string
	for _, i := range names {
		subject = append(subject, fmt.Sprintf("%s=%s", oid[i.Type.String()], i.Value))
	}
	return sep + strings.Join(subject, sep)
}

//func init() {
//	if err := checkForExecutables(serviceCertExecutables); err != nil {
//		panic(err)
//	}
//}
