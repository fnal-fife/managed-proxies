package main

import (
	"context"
	"errors"
	"fmt"
	"os/user"
	"sync"

	//"cdcvs.fnal.gov/discompsupp/ken_proxy_push/experimentutil"
	//"cdcvs.fnal.gov/discompsupp/ken_proxy_push/proxyPushLogger"
	"github.com/jinzhu/copier"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"

	"cdcvs.fnal.gov/discompsupp/ken_proxy_push/experiment"
	"cdcvs.fnal.gov/discompsupp/ken_proxy_push/notifications"
)

// createExptConfig takes the config information from the global file and creates an exptConfig object
//func createExptConfig(expt string) (experimentutil.ExptConfig, error) {
//	var vomsprefix, certfile, keyfile string
//	var c experimentutil.ExptConfig
//
//	exptKey := "experiments." + expt
//	if !viper.IsSet(exptKey) {
//		err := errors.New("Experiment is not configured in the configuration file")
//		log.WithFields(logrus.Fields{
//			"experiment": expt,
//		}).Error(err)
//		return c, err
//	}
//
//	exptSubConfig := viper.Sub(exptKey)
//
//	if exptSubConfig.IsSet("vomsgroup") {
//		vomsprefix = exptSubConfig.GetString("vomsgroup")
//	} else {
//		vomsprefix = viper.GetString("vomsproxyinit.defaultvomsprefixroot") + expt + "/"
//	}
//
//	if exptSubConfig.IsSet("certfile") {
//		certfile = exptSubConfig.GetString("certfile")
//	}
//	if exptSubConfig.IsSet("keyfile") {
//		keyfile = exptSubConfig.GetString("keyfile")
//	}
//
//	// Notifications setup
//	n := notifications.Config{}
//	copier.Copy(&n, &nConfig)
//	n.Experiment = expt
//	n.From = viper.GetString("notifications.admin_email")
//	if !viper.GetBool("test") {
//		n.To = exptSubConfig.GetStringSlice("emails")
//	}
//	n.Subject = "Managed Proxy Push errors for " + expt
//
//	c = experimentutil.ExptConfig{
//		Name:           expt,
//		CertBaseDir:    viper.GetString("global.cert_base_dir"),
//		Krb5ccname:     viper.GetString("global.krb5ccname"),
//		DestDir:        exptSubConfig.GetString("dir"),
//		Nodes:          exptSubConfig.GetStringSlice("nodes"),
//		Accounts:       exptSubConfig.GetStringMapString("accounts"),
//		VomsPrefix:     vomsprefix,
//		CertFile:       certfile,
//		KeyFile:        keyfile,
//		IsTest:         viper.GetBool("test"),
//		NConfig:        n,
//		TimeoutsConfig: tConfig,
//		LogsConfig:     lConfig,
//		VPIConfig:      vConfig,
//		KerbConfig:     krbConfig,
//		PingConfig:     pConfig,
//		SSHConfig:      sConfig,
//		Logger:         proxyPushLogger.New(expt, lConfig),
//	}
//
//	// Put this on to set the notifications logger
//	c.NConfig.Logger = c.Logger
//
//	c.Logger.Debug("Set up experiment config")
//	return c, nil
//
//}

// manageExperimentChannels starts up the various experimentutil.Workers and listens for their response.  It puts these
// statuses into an aggregate channel.
func manageExperimentChannels(ctx context.Context, exptConfigs []experiment.ExptConfig) <-chan experiment.ExperimentSuccess {
	agg := make(chan experiment.ExperimentSuccess, len(exptConfigs))
	var wg, nwg, cwg sync.WaitGroup
	wg.Add(len(exptConfigs))

	// This is the wait group that will trigger closing of the agg channel
	cwg.Add(2)

	// Start all of the experiment workers, put their results into the agg channel
	for _, eConfig := range exptConfigs {
		go func(eConfig experiment.ExptConfig) {
			/* Order of operations in cleanup:
			* Experiment finishes processing, closes notification Manager and experiment success channel
			* Notifications get sent, then nwg is decremented
			* Once nwg and wg are down to 0, we close the agg channel to tell main to cleanup
			 */
			defer wg.Done()
			exptSubConfig := viper.Sub("experiments." + eConfig.Name)

			// Notifications setup
			n := notifications.Config{}
			copier.Copy(&n, &nConfig)
			n.Experiment = eConfig.Name
			if !viper.GetBool("test") {
				n.To = exptSubConfig.GetStringSlice("emails")
			}
			n.Subject = n.Subject + " - " + eConfig.Name

			// Don't let this func return (and thus the aggregate waitgroup decrement) until emails are sent
			nwg.Add(1)
			//defer nwg.Wait()
			nMgr := notifications.NewManager(ctx, &nwg, n)

			exptContext, exptCancel := context.WithTimeout(ctx, tConfig["expttimeoutDuration"])
			defer exptCancel()

			// If all goes well, each experiment Worker channel will be ready to be received on twice:  once when the
			// successful status is sent, and when the channel closes after cleanup.  If we timeout, just move on.
			// Expt channel is buffered anyway, so if the worker tries to send later and there's no receiver,
			// garbage collection will take care of it
			log.WithFields(log.Fields{"experiment": eConfig.Name}).Debug("Starting worker")
			c := experiment.Worker(exptContext, eConfig, promPush, nMgr)
			for {
				select {
				case status, chanOpen := <-c: // Grab status from channel
					if !chanOpen {
						log.WithFields(log.Fields{"experiment": eConfig.Name}).Debug("Experiment channel closed.  Returning.")
						return
					}
					log.WithFields(log.Fields{"experiment": eConfig.Name}).Debug("Received status")
					agg <- status
					log.WithFields(log.Fields{"experiment": eConfig.Name}).Debug("Put status into aggregation channel")

				case <-exptContext.Done():
					if err := exptContext.Err(); err == context.DeadlineExceeded {
						msg := "Timed out waiting for experiment success info to be reported. Someone from USDC should " +
							"look into this and cleanup if needed.  See " +
							"https://cdcvs.fnal.gov/redmine/projects/discompsupp/wiki/MANAGEDPROXIES for instructions."
						log.WithFields(log.Fields{"experiment": eConfig.Name}).Error(msg)
					} else {
						log.WithFields(log.Fields{"experiment": eConfig.Name}).Error(err)
					}
				}
			}
		}(eConfig)
	}

	/* This will wait until all expt workers have put their values into agg channel, and have finished
	sending notifications.

	This prevents the 2 rare race conditions: 1) that main() returns before all expt cleanup
	is done (since main() waits for the agg channel to close before doing cleanup), and 2) we close the
	agg channel before all values have been sent into it.
	*/

	// Wait for all experiment workers to finish
	go func() {
		defer cwg.Done()
		wg.Wait()
	}()

	// Wait for all experiment notification managers to finish
	go func() {
		defer cwg.Done()
		nwg.Wait()
	}()

	// Wait for the previous two goroutines to finish and close agg
	go func() {
		cwg.Wait()
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
		DestDir:        exptSubConfig.GetString("dir"),
		Nodes:          exptSubConfig.GetStringSlice("nodes"),
		Accounts:       exptSubConfig.GetStringMapString("accounts"),
		VomsPrefix:     vomsprefix,
		CertFile:       certfile,
		KeyFile:        keyfile,
		TimeoutsConfig: tConfig,
		KerbConfig:     krbConfig,
	}

	log.WithField("experiment", c.Name).Debug("Set up experiment config")
	return c, nil

}
