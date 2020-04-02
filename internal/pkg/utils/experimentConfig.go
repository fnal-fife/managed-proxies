package utils

import "time"

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
