package proxyutils

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"strings"
	"text/template"
	"time"
)

const defaultValidity = "24h"

var (
	gridProxyExecutables = map[string]string{
		"grid-proxy-init": "",
	}
	gpiArgs     = "-cert {{.CertPath}} -key {{.KeyPath}} -out {{.OutFile}} -valid {{.Valid}}"
	gpiTemplate = template.Must(template.New("grid-proxy-init").Parse(gpiArgs))
)

func init() {
	// Make sure our required executables are in $PATH
	for exe := range gridProxyExecutables {
		if pth, err := exec.LookPath(exe); err != nil {
			fmt.Printf("%s was not found in $PATH.  Exiting", exe)
			os.Exit(1)
		} else {
			gridProxyExecutables[exe] = pth
		}
	}
}

type GridProxy struct {
	Path string
	DN   string
}

func NewGridProxy(s *serviceCert, valid time.Duration) (*GridProxy, error) {
	//TODO implement this.  Want to set time here, maybe.  If so, pass into runGridProxyInit
	// TODO Run checks in this method to make sure we can actually run grid-proxy-init
	// Sanity-check time and set default time if no time given.
	if valid.Seconds() == 0 {
		valid, _ = time.ParseDuration(defaultValidity)
	}

	g, err := s.runGridProxyInit(valid)
	if err != nil {
		fmt.Println("Could not run grid-proxy-init on service cert.")
		return &GridProxy{}, err
	}
	return g, nil
}

// fmtDurationForGPI does TODO .Modified from https://stackoverflow.com/questions/47341278/how-to-format-a-duration-in-golang/47342272#47342272
func fmtDurationForGPI(d time.Duration) string {
	// TODO:  Unit test this.  Throw different combos of hours, minutes, and decimal entries.  Days should fail.
	d = d.Round(time.Minute)
	h := d / time.Hour
	d -= h * time.Hour
	m := d / time.Minute
	return fmt.Sprintf("%d:%02d", h, m)
}

func (s *serviceCert) runGridProxyInit(valid time.Duration) (*GridProxy, error) {
	var b strings.Builder

	_outfile, err := ioutil.TempFile("", "store_managed_proxy_")
	if err != nil {
		fmt.Println("Couldn't get tempfile")
		return &GridProxy{}, err
	}
	outfile := _outfile.Name()

	validDurationStr := fmtDurationForGPI(valid)

	cArgs := struct {
		CertPath string
		KeyPath  string
		OutFile  string
		Valid    string
	}{
		CertPath: s.certPath,
		KeyPath:  s.keyPath,
		OutFile:  outfile,
		Valid:    validDurationStr,
	}

	if err := gpiTemplate.Execute(&b, cArgs); err != nil {
		fmt.Println("Could not execute grid-proxy-init template.")
		return &GridProxy{}, err
	}

	args := strings.Fields(b.String())

	//TODO:  Make this a CommandContext
	cmd := exec.Command(gridProxyExecutables["grid-proxy-init"], args...)
	out, err := cmd.Output()
	if err != nil {
		fmt.Println("Could not execute grid-proxy-init command")
		fmt.Println(err)
		return &GridProxy{}, err
	}
	fmt.Println(out)

	g := GridProxy{Path: outfile}

	_dn, err := getCertSubject(g.Path)
	if err != nil {
		fmt.Println("Could not get proxy subject from grid proxy")
		fmt.Println(err)
		return &GridProxy{}, err
	}
	g.DN = _dn
	return &g, nil
}

func (g *GridProxy) Remove() error {
	err := os.Remove(g.Path)

	if os.IsNotExist(err) {
		fmt.Println("Grid Proxy file does not exist")
	} else if err != nil {
		fmt.Println(err)
	}

	return err
}

func (g *GridProxy) runMyProxyStore(server, retrievers, hours string, s serviceCert) error {
	return nil
}

// Somewhere else, define an interface myProxyer that has a runMyProxyStore func.  Have the func that eventually runs runMyProxyStore run it on the interface.
