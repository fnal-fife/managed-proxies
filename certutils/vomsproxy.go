package proxyutils

import "fmt"

type VomsProxy struct {
	Path string
	VO   string
	Role string
	FQAN string
}

func getVomsProxy(s serviceCert, vomsServer, experiment, role, outfile string) *VomsProxy {
	// TODO:  Need to actually do something here
	fmt.Println("vim-go")
	v := VomsProxy{Path: "path"}
	return &v
}

func (v *VomsProxy) copyProxy(node, account, destdir string) error {
	return nil
}

func (v *VomsProxy) chmodProxy(node, account, destdir string) error {
	return nil
}

// Both of these should be called from an interface rather than the actual object.  Then we can unit test more easily i.e. pushProxyer.copyProxy(....)
