package main

import (
	"context"
	"errors"
	"fmt"
	"os/user"
	"sync"

	"cdcvs.fnal.gov/discompsupp/ken_proxy_push/experimentutil"
	"cdcvs.fnal.gov/discompsupp/ken_proxy_push/notifications"
	"cdcvs.fnal.gov/discompsupp/ken_proxy_push/proxyPushLogger"
	"github.com/jinzhu/copier"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

// createExptConfig takes the config information from the global file and creates an exptConfig object
func createExptConfig(expt string) (experimentutil.ExptConfig, error) {
	var vomsprefix, certfile, keyfile string
	var c experimentutil.ExptConfig

	exptKey := "experiments." + expt
	if !viper.IsSet(exptKey) {
		err := errors.New("Experiment is not configured in the configuration file")
		log.WithFields(logrus.Fields{
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

	// Notifications setup
	n := notifications.Config{}
	copier.Copy(&n, &nConfig)
	n.Experiment = expt
	n.From = viper.GetString("admin_email")
	if !viper.GetBool("test") {
		n.To = exptSubConfig.GetStringSlice("emails")
	}
	n.Subject = "Managed Proxy Push errors for " + expt

	c = experimentutil.ExptConfig{
		Name:           expt,
		CertBaseDir:    viper.GetString("global.cert_base_dir"),
		Krb5ccname:     viper.GetString("global.krb5ccname"),
		DestDir:        exptSubConfig.GetString("dir"),
		Nodes:          exptSubConfig.GetStringSlice("nodes"),
		Accounts:       exptSubConfig.GetStringMapString("accounts"),
		VomsPrefix:     vomsprefix,
		CertFile:       certfile,
		KeyFile:        keyfile,
		IsTest:         viper.GetBool("test"),
		NConfig:        n,
		TimeoutsConfig: tConfig,
		LogsConfig:     lConfig,
		VPIConfig:      vConfig,
		KerbConfig:     krbConfig,
		PingConfig:     pConfig,
		SSHConfig:      sConfig,
		Logger:         proxyPushLogger.New(expt, lConfig),
	}

	// Put this on to set the notifications logger
	c.NConfig.Logger = c.Logger

	c.Logger.Debug("Set up experiment config")
	return c, nil

}

// manageExperimentChannels starts up the various experimentutil.Workers and listens for their response.  It puts these
// statuses into an aggregate channel.
func manageExperimentChannels(ctx context.Context, exptConfigs []experimentutil.ExptConfig) <-chan experimentutil.ExperimentSuccess {
	agg := make(chan experimentutil.ExperimentSuccess, len(exptConfigs))
	var wg sync.WaitGroup
	wg.Add(len(exptConfigs))

	// Start all of the experiment workers, put their results into the agg channel
	for _, eConfig := range exptConfigs {
		go func(eConfig experimentutil.ExptConfig) {
			defer wg.Done()

			exptContext, exptCancel := context.WithTimeout(ctx, tConfig["expttimeoutDuration"])
			defer exptCancel()

			// If all goes well, each experiment Worker channel will be ready to be received on twice:  once when the
			// successful status is sent, and when the channel closes after cleanup.  If we timeout, just move on.
			// Expt channel is buffered anyway, so if the worker tries to send later and there's no receiver,
			// garbage collection will take care of it
			log.WithFields(logrus.Fields{"experiment": eConfig.Name}).Debug("Starting worker")
			c := experimentutil.Worker(exptContext, eConfig, promPush)
			select {
			case status := <-c: // Grab status from channel
				log.WithFields(logrus.Fields{"experiment": eConfig.Name}).Debug("Received status")
				agg <- status
				log.WithFields(logrus.Fields{"experiment": eConfig.Name}).Debug("Put status into aggregation channel")
				<-c // Block until channel closes, which means experiment worker is done with everything
				log.WithFields(logrus.Fields{"experiment": eConfig.Name}).Debug("Experiment channel closed.  Returning.")
			case <-exptContext.Done():
				if err := exptContext.Err(); err == context.DeadlineExceeded {
					msg := "Timed out waiting for experiment success info to be reported. Someone from USDC should " +
						"look into this and cleanup if needed.  See " +
						"https://cdcvs.fnal.gov/redmine/projects/discompsupp/wiki/MANAGEDPROXIES for instructions."
					log.WithFields(logrus.Fields{"experiment": eConfig.Name}).Error(msg)
				} else {
					log.WithFields(logrus.Fields{"experiment": eConfig.Name}).Error(err)
				}
			}
		}(eConfig)
	}

	// This will wait until all expt workers have put their values into agg channel, and have finished
	// cleanup.  This prevents the 2 rare race conditions: 1) that main() returns before all expt cleanup
	// is done (since main() waits for the agg channel to close before doing cleanup), and 2) we close the
	// agg channel before all values have been sent into it.
	go func() {
		wg.Wait()
		log.Debug("Closing aggregation channel")
		close(agg)
	}()

	return agg
}

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

// setAdminEmail sets the notifications config objects' From and To fields to the config file's admin value
func setAdminEmail(pnConfig *notifications.Config) {
	pnConfig.From = pnConfig.ConfigInfo["admin_email"]
	pnConfig.To = []string{pnConfig.ConfigInfo["admin_email"]}
	log.Debug("Set notifications config email values to admin defaults")
	return
}
