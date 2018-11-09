package proxy

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"text/template"
	"time"
)

const (
	defaultValidity  = "24h"
	gpiArgs          = "-cert {{.CertPath}} -key {{.KeyPath}} -out {{.OutFile}} -valid {{.Valid}}"
	myproxystoreArgs = "--certfile {{.CertFile}} --keyfile {{.KeyFile}} -s {{.Server}} -xZ \"{{.Retrievers}}\" -l \"{{.Owner}}\" -t {{.Hours}}"
	//These will move to a config file
	myproxyServer = "fermicloud343.fnal.gov"
)

var (
	gridProxyExecutables = map[string]string{
		"grid-proxy-init": "",
		"myproxy-store":   "",
	}
	gpiTemplate          = template.Must(template.New("grid-proxy-init").Parse(gpiArgs))
	myproxystoreTemplate = template.Must(template.New("myproxy-store").Parse(myproxystoreArgs))
)

func init() {
	checkForExecutables(gridProxyExecutables)
}

type GridProxy struct {
	Path string
	DN   string
	*serviceCert
}

func NewGridProxy(ctx context.Context, s *serviceCert, valid time.Duration) (*GridProxy, error) {
	if valid.Seconds() == 0 {
		valid, _ = time.ParseDuration(defaultValidity)
	}

	g, err := s.runGridProxyInit(ctx, valid)
	if err != nil {
		fmt.Println("Could not run grid-proxy-init on service cert.")
		return &GridProxy{}, err
	}
	return g, nil
}

func (s *serviceCert) runGridProxyInit(ctx context.Context, valid time.Duration) (*GridProxy, error) {
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

	args, err := getArgsFromTemplate(b.String())
	if err != nil {
		fmt.Println("Could not get myproxy-store command arguments from template")
		return &GridProxy{}, err
	}

	cmd := exec.CommandContext(ctx, gridProxyExecutables["grid-proxy-init"], args...)
	out, err := cmd.Output()
	if err != nil {
		fmt.Println("Could not execute grid-proxy-init command")
		fmt.Println(err)
		return &GridProxy{}, err
	}
	fmt.Println(out)

	g := GridProxy{Path: outfile, serviceCert: s}

	_dn, err := getCertSubject(ctx, g.Path)
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

func (g *GridProxy) StoreInMyProxy(ctx context.Context, server string, valid time.Duration) error {
	var b strings.Builder

	hours := strconv.FormatFloat(valid.Hours(), 'f', -1, 32)

	retrievers, err := getRetrievers(ctx)
	if err != nil {
		fmt.Println(err)
		return err
	}

	cArgs := struct{ CertFile, KeyFile, Server, Retrievers, Owner, Hours string }{
		CertFile:   g.Path,
		KeyFile:    g.Path,
		Server:     server,
		Retrievers: retrievers,
		Owner:      g.serviceCert.DN,
		Hours:      hours,
	}

	if err := myproxystoreTemplate.Execute(&b, cArgs); err != nil {
		fmt.Println("Could not execute myproxy-store template")
		return err
	}

	args, err := getArgsFromTemplate(b.String())
	if err != nil {
		fmt.Println("Could not get myproxy-store command arguments from template")
		return err
	}

	env := []string{
		fmt.Sprintf("X509_USER_CERT=%s", g.serviceCert.certPath),
		fmt.Sprintf("X509_USER_KEY=%s", g.serviceCert.keyPath),
	}

	cmd := exec.CommandContext(ctx, gridProxyExecutables["myproxy-store"], args...)
	cmd.Env = env
	out, err := cmd.CombinedOutput()
	if err != nil {
		fmt.Println("Could not execute myproxy-store command.")
	}
	fmt.Println(string(out))
	return err
}

// Somewhere else, define an interface myProxyer that has a runMyProxyStore func.  Have the func that eventually runs runMyProxyStore run it on the interface.

// fmtDurationForGPI does TODO .Modified from https://stackoverflow.com/questions/47341278/how-to-format-a-duration-in-golang/47342272#47342272
func fmtDurationForGPI(d time.Duration) string {
	// TODO:  Unit test this.  Throw different combos of hours, minutes, and decimal entries.  Days should fail.
	d = d.Round(time.Minute)
	h := d / time.Hour
	d -= h * time.Hour
	m := d / time.Minute
	return fmt.Sprintf("%d:%02d", h, m)
}
