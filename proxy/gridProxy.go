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

	"cdcvs.fnal.gov/discompsupp/ken_proxy_push/utils"
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

type GridProxyer interface {
	getGridProxy(context.Context, time.Duration) (*GridProxy, error)
}

// Satisfies the Cert and MyProxyer interfaces
type GridProxy struct {
	Path string
	DN   string
	Cert
}

func NewGridProxy(ctx context.Context, gp GridProxyer, valid time.Duration) (*GridProxy, error) {
	if valid.Seconds() == 0 {
		valid, _ = time.ParseDuration(defaultValidity)
	}

	g, err := gp.getGridProxy(ctx, valid)
	if err != nil {
		return nil, fmt.Errorf("Could not run grid-proxy-init on service cert: %s", err.Error())
	}
	return g, nil
}

func (g *GridProxy) Remove() error {
	if err := os.Remove(g.Path); os.IsNotExist(err) {
		return errors.New("Grid Proxy file does not exist")
	} else {
		return err
	}
}

func (g *GridProxy) storeInMyProxy(ctx context.Context, retrievers, myProxyServer string, valid time.Duration) error {
	var b strings.Builder

	hours := strconv.FormatFloat(valid.Hours(), 'f', -1, 32)

	// Should move to cmd
	//	retrievers, err := GetRetrievers(ctx)
	//	if err != nil {
	//		return fmt.Errorf("Could not get retrievers list from jobsub server: %s", err.Error())
	//	}

	owner, err := g.Cert.getCertSubject(ctx)
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

	args, err := utils.GetArgsFromTemplate(b.String())
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

func (g *GridProxy) getCertSubject(ctx context.Context) (string, error) {
	dn, err := g.Cert.getCertSubject(ctx)
	if err != nil {
		return "", err
	}
	return dn, nil
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

	args, err := utils.GetArgsFromTemplate(b.String())
	if err != nil {
		return &GridProxy{}, fmt.Errorf("Could not get grid-proxy-init command arguments from template: %s", err.Error())
	}

	cmd := exec.CommandContext(ctx, gridProxyExecutables["grid-proxy-init"], args...)
	if err := cmd.Run(); err != nil {
		return &GridProxy{}, fmt.Errorf("Could not execute grid-proxy-init command: %s", err.Error())
	}

	g := GridProxy{Path: outfile, Cert: s}

	_dn, err := g.getCertSubject(ctx)
	if err != nil {
		return &GridProxy{}, fmt.Errorf("Could not get proxy subject from grid proxy: %s", err)
	}
	g.DN = _dn
	return &g, nil
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

func init() {
	if err := utils.CheckForExecutables(gridProxyExecutables); err != nil {
		fmt.Printf("Note that one or more required executables were not found in $PATH: %s\n", err)
	}
}
