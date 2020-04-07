package utils

import (
	"time"

	log "github.com/sirupsen/logrus"
)

// TimeoutsConfig is a map of the timeouts passed in from the config file
type TimeoutsConfig map[string]time.Duration

// KerbConfig contains information needed to run kinit
type KerbConfig map[string]string

// ExptConfig is a mega struct containing all the information the Worker needs to have or pass onto lower level funcs.
type ExptConfig struct {
	Name        string
	CertBaseDir string
	DestDir     string
	Nodes       []string
	Accounts    map[string]string
	VomsPrefix  string
	CertFile    string
	KeyFile     string
	IsTest      bool
	SSHOpts     string
	TimeoutsConfig
	KerbConfig
}

// CreateExptConfig takes the config information from the global file and creates an exptConfig object
// To create functional options, simply define functions that operate on an *ExptConfig.  E.g.
// func foo(e *ExptConfig) { e.Name = "bar" }.  You can then pass in foo to CreateExptConfig (e.g.
// CreateExptConfig("my_expt", foo), to set the ExptConfig.Name to "bar".
//
// To pass in something that's dynamic, define a function that returns a func(*ExptConfig).   e.g.:
// func foo(bar int, e *ExptConfig) func(*ExptConfig) {
//     baz = bar + 3
//     return func(*ExptConfig) {
//	  e.spam = baz
//	}
// If you then pass in foo(3), like CreateExptConfig("my_expt", foo(3)), then ExptConfig.spam will be set to 6
func CreateExptConfig(expt string, options ...func(*ExptConfig)) (*ExptConfig, error) {
	c := ExptConfig{
		Name: expt,
	}

	for _, option := range options {
		option(&c)
	}

	log.WithField("experiment", c.Name).Debug("Set up experiment config")
	return &c, nil
}
