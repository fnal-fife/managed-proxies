package proxy

import (
	"context"
	"errors"
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
	//myproxyServer = "fermicloud343.fnal.gov"
)

var (
	gridProxyExecutables = map[string]string{
		"grid-proxy-init": "",
		"myproxy-store":   "",
	}
	gpiTemplate          = template.Must(template.New("grid-proxy-init").Parse(gpiArgs))
	myproxystoreTemplate = template.Must(template.New("myproxy-store").Parse(myproxystoreArgs))
)

type GridProxy struct {
	Path string
	DN   string
	Cert
}

func NewGridProxy(ctx context.Context, gp gridProxyer, valid time.Duration) (*GridProxy, error) {
	if valid.Seconds() == 0 {
		valid, _ = time.ParseDuration(defaultValidity)
	}

	g, err := gp.getGridProxy(ctx, valid)
	if err != nil {
		return &GridProxy{}, fmt.Errorf("Could not run grid-proxy-init on service cert: %s", err.Error())
	}
	return g, nil
}

func (g *GridProxy) Remove() error {
	err := os.Remove(g.Path)

	if os.IsNotExist(err) {
		return errors.New("Grid Proxy file does not exist")
	}

	return err
}

func (g *GridProxy) StoreInMyProxy(ctx context.Context, retrievers, myProxyServer string, valid time.Duration) error {
	var b strings.Builder

	hours := strconv.FormatFloat(valid.Hours(), 'f', -1, 32)

	// Should move to cmd
	//	retrievers, err := GetRetrievers(ctx)
	//	if err != nil {
	//		return fmt.Errorf("Could not get retrievers list from jobsub server: %s", err.Error())
	//	}

	owner, err := g.Cert.getDN(ctx)
	if err != nil {
		return fmt.Errorf("Could not get cert DN: %s", err.Error())
	}

	cArgs := struct{ CertFile, KeyFile, Server, Retrievers, Owner, Hours string }{
		CertFile:   g.Path,
		KeyFile:    g.Path,
		Server:     myProxyServer,
		Retrievers: retrievers,
		Owner:      owner,
		Hours:      hours,
	}

	if err := myproxystoreTemplate.Execute(&b, cArgs); err != nil {
		return fmt.Errorf("Could not execute myproxy-store template: %s", err.Error())
	}

	args, err := getArgsFromTemplate(b.String())
	if err != nil {
		return fmt.Errorf("Could not get myproxy-store command arguments from template: %s", err)
	}

	env := []string{
		fmt.Sprintf("X509_USER_CERT=%s", g.Cert.getCertPath()),
		fmt.Sprintf("X509_USER_KEY=%s", g.Cert.getKeyPath()),
	}

	cmd := exec.CommandContext(ctx, gridProxyExecutables["myproxy-store"], args...)
	cmd.Env = env
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("Could not execute myproxy-store command: %s", err.Error())
	}
	return nil
}

func (g *GridProxy) getCertPath() string { return g.Cert.getCertPath() }
func (g *GridProxy) getKeyPath() string  { return g.Cert.getKeyPath() }

func (g *GridProxy) getDN(ctx context.Context) (string, error) {
	dn, err := getCertSubject(ctx, g.Path)
	if err != nil {
		return "", err
	}
	return dn, nil
}

type gridProxyer interface {
	getGridProxy(context.Context, time.Duration) (*GridProxy, error)
}

func (s *serviceCert) getGridProxy(ctx context.Context, valid time.Duration) (*GridProxy, error) {
	var b strings.Builder

	_outfile, err := ioutil.TempFile("", "managed_proxy_grid_")
	if err != nil {
		return &GridProxy{}, errors.New("Couldn't get tempfile")
	}
	outfile := _outfile.Name()

	validDurationStr := fmtDurationForGPI(valid)

	cArgs := struct{ CertPath, KeyPath, OutFile, Valid string }{
		CertPath: s.certPath,
		KeyPath:  s.keyPath,
		OutFile:  outfile,
		Valid:    validDurationStr,
	}

	if err := gpiTemplate.Execute(&b, cArgs); err != nil {
		return &GridProxy{}, fmt.Errorf("Could not execute grid-proxy-init template: %s", err.Error())
	}

	args, err := getArgsFromTemplate(b.String())
	if err != nil {
		return &GridProxy{}, fmt.Errorf("Could not get grid-proxy-init command arguments from template: %s", err.Error())
	}

	cmd := exec.CommandContext(ctx, gridProxyExecutables["grid-proxy-init"], args...)
	if err := cmd.Run(); err != nil {
		return &GridProxy{}, fmt.Errorf("Could not execute grid-proxy-init command: %s", err.Error())
	}

	g := GridProxy{Path: outfile, Cert: s}

	_dn, err := g.getDN(ctx)
	if err != nil {
		return &GridProxy{}, fmt.Errorf("Could not get proxy subject from grid proxy: %s", err)
	}
	g.DN = _dn
	return &g, nil
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

func init() { checkForExecutables(gridProxyExecutables) }
