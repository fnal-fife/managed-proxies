package proxyutils

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
)

const (
	// These will all move to a config file
	caPath                = "/etc/grid-security/certificates/"
	jobsubServer          = "fifebatch.fnal.gov"
	cigetcertoptsEndpoint = "cigetcertopts.txt"
	defaultRetrievers     = "(/DC=com/DC=DigiCert-Grid|/DC=org/DC=opensciencegrid)/O=Open Science Grid/OU=Services/CN=fermicloud(0|1|2|3|4|5|6|7|8|9)(0|1|2|3|4|5|6|7|8|9)(0|1|2|3|4|5|6|7|8|9).fnal.gov"
)

var HTTPSClient *http.Client

// TODO:  return error?
func startHTTPSClient() {
	// HTTPS client
	caCertSlice := make([]string, 0)
	caCertPool := x509.NewCertPool()

	// Adapted from  https://gist.github.com/michaljemala/d6f4e01c4834bf47a9c4
	// Load CA certs
	caFiles, err := ioutil.ReadDir(caPath)
	if err != nil {
		//log.Error(err)
		fmt.Println(err)

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
			//	log.Error(err)
			fmt.Println(err)
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

}

func getCigetcertopts(ctx context.Context, server, endpoint string) ([]byte, error) {
	// Set serverString here, (e.g. https://jobsubserver.fnal.gov:1234/), since that will be constant for the
	// run.
	data := make([]byte, 0)
	queryEndpoint := fmt.Sprintf("https://%s/%s", server, endpoint)

	req, err := http.NewRequest("GET", queryEndpoint, nil)
	if err != nil {
		fmt.Println(err)
		return data, err
	}
	req = req.WithContext(ctx)

	//	c := make(chan struct {
	//		resp *http.Response
	//		err  error
	//	}, 1)
	//
	var r *http.Response

	if r, err = HTTPSClient.Do(req); err != nil {
		fmt.Println(err)
		return data, err
	}

	//
	//
	//	go func() {
	//		resp, err := HTTPSClient.Do(req)
	//		c <- struct {
	//			resp *http.Response
	//			err  error
	//		}{resp, err}
	//	}()
	//
	//	select {
	//	case <-ctx.Done():
	//		rstruct := <-c
	//	case rstruct := <-c:
	//		if rstruct.err != nil {
	//		}
	//		r = rstruct.resp
	//	}

	defer r.Body.Close()

	data, err = ioutil.ReadAll(r.Body)
	if err != nil {
		fmt.Println(err)
		data = []byte{}
		return data, err
	}
	return data, err

}

func getRetrievers(ctx context.Context) (string, error) {
	startHTTPSClient()

	data, err := getCigetcertopts(ctx, jobsubServer, cigetcertoptsEndpoint)
	if err != nil {
		fmt.Println("Error getting cigetcertopts from jobsub server")
		return "", err
	}

	retrieversRegExp := regexp.MustCompile("^--myproxyretrievers='(.+)'$")
	retrieversSlice := make([]string, 0)

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
			return "", errors.New("Too many matches within line.  Check file")
		}
	}
	if len(retrieversSlice) > 1 {
		return "", errors.New("Too many matches within file.  Check file")
	}
	return retrieversSlice[0], nil
}

func checkRetrievers(retrievers, defaultRetrievers string) error {
	if retrievers != defaultRetrievers {
		return errors.New("Attention:  The retrievers on the jobsub server do not match the default retrievers")
	}
	return nil
}
