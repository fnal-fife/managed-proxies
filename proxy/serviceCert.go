package proxy

import (
	"context"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"io/ioutil"
	"strings"
)

type Cert interface {
	getCertSubject(context.Context) (string, error)
	getCertPath() string
	getKeyPath() string
}

func GetDN(ctx context.Context, c Cert) (string, error) {
	dn, err := c.getCertSubject(ctx)
	if err != nil {
		return "", err
	}
	return dn, err
}

// Satisifies the Cert and GridProxyer interfaces
type serviceCert struct {
	certPath string
	keyPath  string
	DN       string
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
