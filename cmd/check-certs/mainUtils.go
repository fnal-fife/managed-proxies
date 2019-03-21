package main

import (
	"errors"
	"fmt"
	"os/user"
	"regexp"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"

	"cdcvs.fnal.gov/discompsupp/ken_proxy_push/experiment"
	"cdcvs.fnal.gov/discompsupp/ken_proxy_push/notifications"
)

var emailRegexp = regexp.MustCompile(`^[\w\._%+-]+@[\w\.-]+\.\w{2,}$`)

// createExptConfig takes the config information from the global file and creates an exptConfig object
func createExptConfig(expt string) (experiment.ExptConfig, error) {
	var vomsprefix, certfile, keyfile string
	var c experiment.ExptConfig

	exptKey := "experiments." + expt
	if !viper.IsSet(exptKey) {
		err := errors.New("Experiment is not configured in the configuration file")
		log.WithFields(log.Fields{
			"experiment": expt,
		}).Error(err)
		return c, err
	}

	exptSubConfig := viper.Sub(exptKey)

	if exptSubConfig.IsSet("vomsgroup") {
		vomsprefix = exptSubConfig.GetString("vomsgroup")
	} else {
		vomsprefix = viper.GetString("vomsproxyinit.defaultvomsprefixroot") + expt + "/"
	}

	if exptSubConfig.IsSet("certfile") {
		certfile = exptSubConfig.GetString("certfile")
	}
	if exptSubConfig.IsSet("keyfile") {
		keyfile = exptSubConfig.GetString("keyfile")
	}

	c = experiment.ExptConfig{
		Name:           expt,
		CertBaseDir:    viper.GetString("global.cert_base_dir"),
		Accounts:       exptSubConfig.GetStringMapString("accounts"),
		CertFile:       certfile,
		KeyFile:        keyfile,
		TimeoutsConfig: tConfig,
		IsTest:         viper.GetBool("test"),
	}

	log.WithField("experiment", c.Name).Debug("Set up experiment config")
	return c, nil

}

// setAdminEmail sets the notifications config objects' From and To fields to the config file's admin value
func setAdminEmail(pnConfig *notifications.Config) {
	var toEmail string
	pnConfig.From = pnConfig.ConfigInfo["admin_email"]

	if viper.GetString("admin") != "" {
		toEmail = viper.GetString("admin")
	} else {
		toEmail = pnConfig.ConfigInfo["admin_email"]
	}

	pnConfig.To = []string{toEmail}
	log.Debug("Set notifications config email values to admin values")
	return
}

// checkUser makes sure that the user running the executable is the authorized user.
func checkUser(authuser string) error {
	cuser, err := user.Current()
	if err != nil {
		return errors.New("Could not lookup current user.  Exiting")
	}
	log.Debug("Running script as ", cuser.Username)
	if cuser.Username != authuser {
		return fmt.Errorf("This must be run as %s.  Trying to run as %s", authuser, cuser.Username)
	}
	return nil
}
