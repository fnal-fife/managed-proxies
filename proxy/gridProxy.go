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

	"cdcvs.fnal.gov/discompsupp/ken_proxy_push/v3/internal/pkg/utils"
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

// getGridProxyer is the interface for types that support getting a grid proxy
type getGridProxyer interface {
	getGridProxy(context.Context, time.Duration) (*GridProxy, error)
}

// GridProxy contains the path to the grid proxy file, the DN of the proxy, and the Cert object used to create the proxy
// Satisfies the MyProxyer interfaces
type GridProxy struct {
	Path string
	DN   string
	Cert
}

// NewGridProxyError TODO
type NewGridProxyError struct {
	message string
}

func (ne *NewGridProxyError) Error() string {
	return fmt.Sprintf("Could not generate grid proxy: %s", ne.message)
}

// NewGridProxy returns a new GridProxy object, a teardown func, and an error, given a getGridProxyer object and the lifetime of the intended proxy
func NewGridProxy(ctx context.Context, gp getGridProxyer, valid time.Duration) (*GridProxy, func() error, error) {
	if valid.Seconds() == 0 {
		valid, _ = time.ParseDuration(defaultValidity)
	}

	teardown := func() error { return nil }

	g, err := gp.getGridProxy(ctx, valid)
	if err != nil {
		err := &NewGridProxyError{"Could not get a new grid proxy from gridProxyer"}
		log.WithField("gridProxyer", fmt.Sprintf("%v", gp)).Error(err)
		return nil, teardown, err
	}
	log.WithFields(log.Fields{
		"path": g.Path,
		"DN":   g.DN,
	}).Debug("Generated new GridProxy")

	teardown = func() error {
		if err := os.Remove(g.Path); os.IsNotExist(err) {
			errText := "Grid proxy file does not exist"
			log.WithField("path", g.Path).Error(errText)
			return err
		} else if err != nil {
			log.WithField("path", g.Path).Error(err)
			return err
		}

		log.WithField("path", g.Path).Debug("Grid Proxy removed")
		return nil
	}

	return g, teardown, nil
}

// storeInMyProxy stores a GridProxy object on a myproxy server by using myproxy-store
func (g *GridProxy) storeInMyProxy(ctx context.Context, retrievers, myProxyServer string, valid time.Duration) error {
	var b strings.Builder

	hours := strconv.FormatFloat(valid.Hours(), 'f', -1, 32)

	owner := g.Subject()

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
		fmt.Sprintf("X509_USER_CERT=%s", g.CertPath()),
		fmt.Sprintf("X509_USER_KEY=%s", g.KeyPath()),
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

// getGridProxy returns a GridProxy given a ServiceCert object
func (s *ServiceCert) getGridProxy(ctx context.Context, valid time.Duration) (*GridProxy, error) {
	var b strings.Builder

	_outfile, err := ioutil.TempFile("", "managed_proxy_grid_")
	if err != nil {
		err := fmt.Sprintf("Couldn't get tempfile: %s", err.Error())
		log.WithField("certPath", s.certPath).Error(err)
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
		log.WithField("certPath", s.certPath).Error(err)
		return &GridProxy{}, errors.New(err)
	}

	args, err := utils.GetArgsFromTemplate(b.String())
	if err != nil {
		err := fmt.Sprintf("Could not get grid-proxy-init command arguments from template: %s", err.Error())
		log.WithField("certPath", s.certPath).Error(err)
		return &GridProxy{}, errors.New(err)
	}

	cmd := exec.CommandContext(ctx, gridProxyExecutables["grid-proxy-init"], args...)
	if err := cmd.Run(); err != nil {
		err := fmt.Sprintf("Could not execute grid-proxy-init command: %s", err.Error())
		log.WithFields(log.Fields{
			"certPath": cArgs.CertPath,
			"command":  strings.Join(cmd.Args, " "),
		}).Error(err)
		return &GridProxy{}, errors.New(err)
	}

	g := GridProxy{Path: outfile, Cert: s}

	g.DN = g.Subject()
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
