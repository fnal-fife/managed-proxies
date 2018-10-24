package proxyutils

import "fmt"

type serviceCert struct {
	certPath string
	keyPath  string
	DN       string
}

func NewServiceCert(certPath, keyPath string) *serviceCert {
	fmt.Println("Ingesting service cert")
	dn, err := getCertSubject(certPath)
	if err != nil {
		fmt.Println("Handle this")
	}

	return &serviceCert{certPath, keyPath, dn}
}

func getCertSubject(certPath string) (string, error) {
	//TODO Do something
	return "DN", nil
}
