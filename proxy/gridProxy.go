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

	log "github.com/sirupsen/logrus"

	"cdcvs.fnal.gov/discompsupp/ken_proxy_push/utils"
)

const (
	defaultValidity  = "24h"
	gpiArgs          = "-cert {{.CertPath}} -key {{.KeyPath}} -out {{.OutFile}} -valid {{.Valid}}"
	myproxystoreArgs = "--certfile {{.CertFile}} --keyfile {{.KeyFile}} -s {{.Server}} -xZ \"{{.Retrievers}}\" -l \"{{.Owner}}\" -t {{.Hours}}"
)

var (
	gridProxyExecutables = map[string]string{
		"grid-proxy-init": "",
		"myproxy-store":   "",
	}
	gpiTemplate          = template.Must(template.New("grid-proxy-init").Parse(gpiArgs))
	myproxystoreTemplate = template.Must(template.New("myproxy-store").Parse(myproxystoreArgs))
)

// GridProxyer is the interface for types that support getting a grid proxy
type GridProxyer interface {
	getGridProxy(context.Context, time.Duration) (*GridProxy, error)
}

// GridProxy contains the path to the grid proxy file, the DN of the proxy, and the Cert object used to create the proxy
// Satisfies the Cert and MyProxyer interfaces
type GridProxy struct {
	Path string
	DN   string
	Cert
}

// NewGridProxy returns a new GridProxy object given a GridProxyer object and the lifetime of the intended proxy
func NewGridProxy(ctx context.Context, gp GridProxyer, valid time.Duration) (*GridProxy, error) {
	if valid.Seconds() == 0 {
		valid, _ = time.ParseDuration(defaultValidity)
	}

	g, err := gp.getGridProxy(ctx, valid)
	if err != nil {
		err := "Could not get a new grid proxy from gridProxyer"
		log.WithField("gridProxyer", fmt.Sprintf("%v", gp)).Error(err)
		return nil, errors.New(err)
	}
	log.WithFields(log.Fields{
		"path": g.Path,
		"DN":   g.DN,
	}).Debug("Generated new GridProxy")
	return g, nil
}

// Remove deletes the file at GridProxy.Path
func (g *GridProxy) Remove() error {
	if err := os.Remove(g.Path); os.IsNotExist(err) {
		err := "Grid proxy file does not exist"
		log.WithField("path", g.Path).Error(err)
		return errors.New(err)
	} else if err != nil {
		log.WithField("path", g.Path).Error(err)
		return err
	}

	log.WithField("path", g.Path).Debug("Grid Proxy removed")
	return nil
}

// storeInMyProxy stores a GridProxy object on a myproxy server by using myproxy-store
func (g *GridProxy) storeInMyProxy(ctx context.Context, retrievers, myProxyServer string, valid time.Duration) error {
	var b strings.Builder

	hours := strconv.FormatFloat(valid.Hours(), 'f', -1, 32)

	owner, err := g.Cert.getCertSubject(ctx)
	if err != nil {
		err := "Could not get cert subject to store in myproxy"
		log.WithField("certPath", g.Cert.getCertPath()).Error(err)
		return errors.New(err)
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
		err := fmt.Sprintf("Could not execute myproxy-store template: %s", err.Error())
		log.WithField("gridProxy", g.DN).Error(err)
		return errors.New(err)
	}

	args, err := utils.GetArgsFromTemplate(b.String())
	if err != nil {
		err := fmt.Sprintf("Could not get myproxy-store command arguments from template: %s", err.Error())
		log.WithField("gridProxy", g.DN).Error(err)
		return errors.New(err)
	}

	env := []string{
		fmt.Sprintf("X509_USER_CERT=%s", g.Cert.getCertPath()),
		fmt.Sprintf("X509_USER_KEY=%s", g.Cert.getKeyPath()),
	}

	cmd := exec.CommandContext(ctx, gridProxyExecutables["myproxy-store"], args...)
	cmd.Env = env
	if stdOutstdErr, err := cmd.CombinedOutput(); err != nil {
		err := fmt.Sprintf("Could not execute myproxy-store command: %s", stdOutstdErr)
		log.WithFields(log.Fields{
			"gridProxy": g.DN,
			"command":   strings.Join(cmd.Args, " "),
		}).Error(err)
		return errors.New(err)
	}
	log.WithFields(log.Fields{
		"gridProxy":     g.DN,
		"myProxyServer": myProxyServer,
		"validHours":    hours,
	}).Debug("Successfully stored grid proxy in myproxy")

	return nil
}

func (g *GridProxy) getCertPath() string { return g.Cert.getCertPath() }
func (g *GridProxy) getKeyPath() string  { return g.Cert.getKeyPath() }

func (g *GridProxy) getCertSubject(ctx context.Context) (string, error) {
	dn, err := g.Cert.getCertSubject(ctx)
	if err != nil {
		err := "Could not get subject for grid proxy"
		log.WithField("certPath", g.Cert.getCertPath()).Error(err)
		return "", errors.New(err)
	}
	return dn, nil
}

// getGridProxy returns a GridProxy given a serviceCert object
func (s *serviceCert) getGridProxy(ctx context.Context, valid time.Duration) (*GridProxy, error) {
	var b strings.Builder

	_outfile, err := ioutil.TempFile("", "managed_proxy_grid_")
	if err != nil {
		err := fmt.Sprintf("Couldn't get tempfile: %s", err.Error())
		log.WithField("certPath", s.getCertPath()).Error(err)
		return &GridProxy{}, errors.New(err)
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
		err := fmt.Sprintf("Could not execute grid-proxy-init template: %s", err.Error())
		log.WithField("certPath", s.getCertPath()).Error(err)
		return &GridProxy{}, errors.New(err)
	}

	args, err := utils.GetArgsFromTemplate(b.String())
	if err != nil {
		err := fmt.Sprintf("Could not get grid-proxy-init command arguments from template: %s", err.Error())
		log.WithField("certPath", s.getCertPath()).Error(err)
		return &GridProxy{}, errors.New(err)
	}

	cmd := exec.CommandContext(ctx, gridProxyExecutables["grid-proxy-init"], args...)
	if err := cmd.Run(); err != nil {
		err := fmt.Sprintf("Could not execute grid-proxy-init command: %s", err.Error())
		log.WithFields(log.Fields{
			"certPath": s.getCertPath(),
			"command":  strings.Join(cmd.Args, " "),
		}).Error(err)
		return &GridProxy{}, errors.New(err)
	}

	g := GridProxy{Path: outfile, Cert: s}

	_dn, err := g.getCertSubject(ctx)
	if err != nil {
		err := "Could not get proxy subject from grid proxy"
		log.WithField("certPath", s.getCertPath()).Error(err)
		return &GridProxy{}, errors.New(err)
	}
	g.DN = _dn
	log.WithFields(log.Fields{
		"path":    g.Path,
		"subject": g.DN,
	}).Debug("Successfully generated grid proxy")
	return &g, nil
}

// fmtDurationForGPI formats a time.Duration object into a string that is formatted for use in grid proxy commands. Modified from https://stackoverflow.com/questions/47341278/how-to-format-a-duration-in-golang/47342272#47342272
func fmtDurationForGPI(d time.Duration) string {
	d = d.Round(time.Minute)
	h := d / time.Hour
	d -= h * time.Hour
	m := d / time.Minute
	return fmt.Sprintf("%d:%02d", h, m)
}

func init() {
	if err := utils.CheckForExecutables(gridProxyExecutables); err != nil {
		log.WithField("executableGroup", "gridProxy").Error("One or more required executables were not found in $PATH.  Will still attempt to run, but this will probably fail")
	}
}
