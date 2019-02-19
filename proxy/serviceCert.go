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
	"time"

	log "github.com/sirupsen/logrus"
)

// Cert encapsulates the methods that would be normally performed on a certificate to get information
type Cert interface {
	getCertSubject(context.Context) (string, error)
	getCertExpiration(context.Context) (time.Time, error)
	getCertPath() string
	getKeyPath() string
}

// GetDN is a function that accepts any object that satisifies the Cert interface and returns its DN
func GetDN(ctx context.Context, c Cert) (string, error) {
	dn, err := c.getCertSubject(ctx)
	if err != nil {
		log.WithField("path", c.getCertPath()).Error("Could not get certificate subject")
		return "", err
	}
	return dn, err
}

// serviceCert is an object that collects the pertinent information about a service certificate
// Satisifies the Cert, GridProxyer, and VOMSProxyer interfaces
type serviceCert struct {
	certPath   string
	keyPath    string
	DN         string
	Expiration time.Time
}

// NewServiceCert ingests a service certificate file and returns a pointer to a serviceCert object
func NewServiceCert(ctx context.Context, certPath, keyPath string) (*serviceCert, error) {
	s := &serviceCert{
		certPath: certPath,
		keyPath:  keyPath,
	}

	dn, err := s.getCertSubject(ctx)
	if err != nil {
		err := "Could not get DN from certificate"
		log.WithField("certPath", certPath).Error(err)
		return nil, errors.New(err)
	}
	s.DN = dn

	expires, err := s.getCertExpiration(ctx)
	if err != nil {
		err := "Could not get expiration date from certificate"
		log.WithField("certPath", certPath).Error(err)
		return nil, errors.New(err)
	}
	s.Expiration = expires

	log.WithFields(log.Fields{
		"certPath":   certPath,
		"subject":    s.DN,
		"expiration": s.Expiration,
	}).Debug("Successfully ingested service certificate")
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
		err := fmt.Sprintf("Could not read cert file: %s", err.Error())
		log.WithField("certPath", s.certPath).Error(err)
		return "", errors.New(err)
	}

	certDER, _ := pem.Decode(certContent)
	if certDER == nil {
		err := "Could not decode PEM block containing cert"
		log.WithField("certPath", s.certPath).Error(err)
		return "", errors.New(err)
	}

	cert, err := x509.ParseCertificate(certDER.Bytes)
	if err != nil {
		err := "Could not parse certificate from DER data"
		log.WithField("certPath", s.certPath).Error(err)
		return "", errors.New(err)
	}

	log.WithField("certPath", s.certPath).Debug("Read in and decoded cert file, will now find subject")
	return parseDN(cert.Subject.Names, "/"), nil
}

func (s *serviceCert) getCertExpiration(ctx context.Context) (time.Time, error) {
	var t time.Time
	certContent, err := ioutil.ReadFile(s.certPath)
	if err != nil {
		err := fmt.Sprintf("Could not read cert file: %s", err.Error())
		log.WithField("certPath", s.certPath).Error(err)
		return t, errors.New(err)
	}

	certDER, _ := pem.Decode(certContent)
	if certDER == nil {
		err := "Could not decode PEM block containing cert"
		log.WithField("certPath", s.certPath).Error(err)
		return t, errors.New(err)
	}

	cert, err := x509.ParseCertificate(certDER.Bytes)
	if err != nil {
		err := "Could not parse certificate from DER data"
		log.WithField("certPath", s.certPath).Error(err)
		return t, errors.New(err)
	}

	log.WithField("certPath", s.certPath).Debug("Read in and decoded cert file, will now find expiration")
	return cert.NotAfter, nil
}

// Thank you FERRY for this.  names can be *x509.Certificate.Subject.Names object
// parseDN takes a []pkix.AttributeTypeAndValue slice (like the elements of a cert Subject), the separator, and returns a DN, formatted in the openssl format
func parseDN(names []pkix.AttributeTypeAndValue, sep string) string {
	var oid = map[string]string{
		"2.5.4.3":                    "CN",
		"2.5.4.4":                    "SN",
		"2.5.4.5":                    "serialNumber",
		"2.5.4.6":                    "C",
		"2.5.4.7":                    "L",
		"2.5.4.8":                    "ST",
		"2.5.4.9":                    "STREET",
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
		"0.9.2342.19200300.100.1.1":  "UID",
	}
	var subject []string
	for _, i := range names {
		subject = append(subject, fmt.Sprintf("%s=%s", oid[i.Type.String()], i.Value))
	}
	return sep + strings.Join(subject, sep)
}
