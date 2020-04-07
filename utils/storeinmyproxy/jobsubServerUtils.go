package storeinmyproxy

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"path/filepath"
	"regexp"

	log "github.com/sirupsen/logrus"
)

// HTTPSClient is a client that will be used to make various https connections
var HTTPSClient *http.Client

// StartHTTPSClient creates an *http.Client and assigns it to HTTPSClient
func StartHTTPSClient(caPath string) error {
	// HTTPS client
	caCertSlice := make([]string, 0)
	caCertPool := x509.NewCertPool()

	// Adapted from  https://gist.github.com/michaljemala/d6f4e01c4834bf47a9c4
	// Load CA certs
	caFiles, err := ioutil.ReadDir(caPath)
	if err != nil {
		log.WithField("caPath", caPath).Error(err)
		return err
	}

	for _, f := range caFiles {
		if filepath.Ext(f.Name()) == ".pem" {
			filenameToAdd := caPath + f.Name()
			caCertSlice = append(caCertSlice, filenameToAdd)
		}
	}

	for _, f := range caCertSlice {
		caCert, err := ioutil.ReadFile(f)
		if err != nil {
			log.WithField("CA_Cert_file", f).Error(err)
			return err
		}
		caCertPool.AppendCertsFromPEM(caCert)
	}

	// Set up HTTPS client
	tlsConfig := &tls.Config{
		RootCAs:       caCertPool,
		Renegotiation: tls.RenegotiateFreelyAsClient,
	}

	tlsConfig.BuildNameToCertificate()
	transport := &http.Transport{TLSClientConfig: tlsConfig}
	HTTPSClient = &http.Client{Transport: transport}

	return nil
}

// GetRetrievers uses HTTPSClient to contact a jobsub server, and retrieves the myproxyretrievers string from the cigetcertOptsEndpoint
func GetRetrievers(ctx context.Context, jobsubServer, cigetcertOptsEndpoint string) (string, error) {
	if HTTPSClient == nil {
		return "", errors.New("HTTPS Client was not started")
	}

	data, err := getCigetcertopts(ctx, jobsubServer, cigetcertOptsEndpoint)
	if err != nil {
		err := fmt.Sprintf("Error getting cigetcertopts from jobsub server: %s", err.Error())
		log.WithFields(log.Fields{
			"jobsubServer":          jobsubServer,
			"cigetcertOptsEndpoint": cigetcertOptsEndpoint,
		}).Error(err)
		return "", errors.New(err)
	}

	retrieversRegExp := regexp.MustCompile("^--myproxyretrievers='(.+)'$")
	retrieversSlice := make([]string, 0)

	_f := func() error {
		err := "Too many matches within line.  Check file"
		log.Error(err)
		return errors.New(err)
	}

	scanner := bufio.NewScanner(bytes.NewReader(data))
	for scanner.Scan() {
		text := string(scanner.Bytes())
		matches := retrieversRegExp.FindAllStringSubmatch(text, -1)
		numMatches := len(matches)
		switch numMatches {
		case 0:
			continue
		case 1:
			retrieversSlice = append(retrieversSlice, matches[0][1])
		default:
			// More than one match - this is bad
			return "", _f()
		}
	}
	if len(retrieversSlice) > 1 {
		return "", _f()
	}
	return retrieversSlice[0], nil
}

// CheckRetrievers is a helper function that simply checks if the myproxyretrievers and expected retrievers match
func CheckRetrievers(retrievers, defaultRetrievers string) error {
	if retrievers != defaultRetrievers {
		err := "Attention:  The retrievers on the jobsub server do not match the default retrievers"
		log.WithFields(log.Fields{
			"retrievers":        retrievers,
			"defaultRetrievers": defaultRetrievers,
		}).Error(err)
		return errors.New(err)
	}
	return nil
}

// getCigetcertopts connects to a web server at the specified endpoint using HTTPSClient, and returns the response if it is valid
func getCigetcertopts(ctx context.Context, server, endpoint string) ([]byte, error) {
	data := make([]byte, 0)
	queryEndpoint := fmt.Sprintf("https://%s/%s", server, endpoint)

	req, err := http.NewRequest("GET", queryEndpoint, nil)
	if err != nil {
		err := fmt.Sprintf("Could not create new HTTP request.  Error was %s", err.Error())
		log.WithFields(log.Fields{
			"server":        server,
			"endpoint":      endpoint,
			"queryEndpoint": queryEndpoint,
		}).Error(err)
		return data, errors.New(err)
	}
	req = req.WithContext(ctx)

	var r *http.Response

	if r, err = HTTPSClient.Do(req); err != nil {
		err := fmt.Sprintf("Could not execute HTTP request.  Error was %s", err.Error())
		log.WithFields(log.Fields{
			"server":        server,
			"endpoint":      endpoint,
			"queryEndpoint": queryEndpoint,
		}).Error(err)
		return []byte{}, errors.New(err)
	}

	defer r.Body.Close()

	data, err = ioutil.ReadAll(r.Body)
	if err != nil {
		err := fmt.Sprintf("Could not read response body.  Error was %s", err.Error())
		log.WithFields(log.Fields{
			"server":        server,
			"endpoint":      endpoint,
			"queryEndpoint": queryEndpoint,
		}).Error(err)
		data = []byte{}
		return data, errors.New(err)
	}
	return data, err
}
