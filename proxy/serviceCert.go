package proxy

import (
	"context"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

// Cert encapsulates the methods that would be normally performed on a certificate to get information
type Cert interface {
	Subject() string
	Expires() time.Time
	CertPath() string
	KeyPath() string
}

// serviceCert is an object that collects the pertinent information about a service certificate
// Satisifies the Cert, GetGridProxyer, and GetVomsProxyer interfaces
type serviceCert struct {
	certPath   string
	keyPath    string
	dn         string
	expiration time.Time
}

// NewServiceCert ingests a service certificate file and returns a pointer to a serviceCert object
func NewServiceCert(ctx context.Context, certPath, keyPath string) (*serviceCert, error) {
	s := &serviceCert{
		certPath: certPath,
		keyPath:  keyPath,
	}

	certFile, err := os.Open(s.certPath)
	if err != nil {
		if os.IsNotExist(err) {
			errText := fmt.Sprintf("certPath does not exist")
			log.WithField("certPath", s.certPath).Error(errText)
			return s, err
		}
		err := &OpenCertFileError{err.Error()}
		log.WithField("certPath", s.certPath).Error(err)
		return s, err
	}
	defer certFile.Close()

	cert, err := IngestCertificate(certFile)
	if err != nil {
		err := fmt.Sprintf("Could not ingest cert file: %s", err.Error())
		log.WithField("certPath", s.certPath).Error(err)
		return s, errors.New(err)
	}

	log.WithField("certPath", s.certPath).Debug("Read in and decoded cert file, getting dn and expiration")
	s.dn = parseDN(cert.Subject.Names, "/")
	s.expiration = cert.NotAfter

	log.WithFields(log.Fields{
		"certPath":   certPath,
		"subject":    s.dn,
		"expiration": s.expiration,
	}).Debug("Successfully ingested service certificate")
	return s, nil
}

func (s *serviceCert) CertPath() string   { return s.certPath }
func (s *serviceCert) KeyPath() string    { return s.keyPath }
func (s *serviceCert) Subject() string    { return s.dn }
func (s *serviceCert) Expires() time.Time { return s.expiration }

// IngestCertificate takes an io.Reader representing a DER-encoded x509 certificate and returns an x509.Certificate object
func IngestCertificate(r io.Reader) (*x509.Certificate, error) {
	certContent, err := ioutil.ReadAll(r)
	if err != nil {
		err := &IngestError{fmt.Sprintf("Could not read cert file: %s", err.Error())}
		log.Error(err)
		return &x509.Certificate{}, err
	}

	certDER, _ := pem.Decode(certContent)
	if certDER == nil {
		err := &IngestError{"Could not decode PEM block containing cert data"}
		return &x509.Certificate{}, err
	}

	cert, err := x509.ParseCertificate(certDER.Bytes)
	if err != nil {
		err := &IngestError{fmt.Sprintf("Could not parse certificate from DER data: %s", err.Error())}
		return cert, err
	}
	return cert, nil
}

// Thank you FERRY for this.  names can be *x509.Certificate.Subject.Names object
// parsedn takes a []pkix.AttributeTypeAndValue slice (like the elements of a cert Subject), the separator, and returns a dn, formatted in the openssl format
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

// IngestError TODO
type IngestError struct {
	message string
}

func (oc *IngestError) Error() string {
	return fmt.Sprintf("Could not ingest cert data: %s", oc.message)
}

// OpenCertFileError TODO
type OpenCertFileError struct {
	message string
}

func (oc *OpenCertFileError) Error() string {
	return fmt.Sprintf("Could not open cert file: %s", oc.message)
}
