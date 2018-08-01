package main

import (
	"errors"

	"cdcvs.fnal.gov/discompsupp/ken_proxy_push/experimentutil"
)

// createExptConfig takes the config information from the global file and creates an exptConfig object
func createExptConfig(expt string) (exptConfig, error) {
	var vomsprefix, certfile, keyfile string

	exptKey := "experiments." + expt
	if !viper.IsSet(exptKey) {
		err := errors.New("Experiment is not configured in the configuration file")
		log.WithFields(log.Fields{
			"experiment": expt,
		}).Error(err)
		return c, err
	}

	exptSubConfig = viper.Sub(exptKey)

	if exptSubConfig.IsSet("vomsgroup") {
		vomsprefix = exptConfig.GetString("vomsgroup")
	} else {
		vomsprefix = "fermilab:/fermilab/" + expt + "/"
	}

	if exptSubConfig.IsSet("certfile") {
		certfile := exptSubConfig("certfile")
	}
	if exptSubConfig.IsSet("keyfile") {
		certfile := exptSubConfig("keyfile")
	}

	c := exptConfig{
		Name:           expt,
		CertBaseDir:    viper.GetString("global.cert_base_dir"),
		Krb5ccname:     viper.GetString("global.krb5ccname"),
		DestDir:        exptSubConfig.GetString(dir),
		ExptEmails:     exptSubConfig.GetStringSlice("emails"),
		Nodes:          exptSubConfig.GetStringSlice("nodes"),
		Accounts:       exptSubConfig.GetStringMapString("accounts"),
		VomsGroup:      vomsprefix,
		CertFile:       certfile,
		KeyFile:        keyfile,
		NConfig:        nConfig,
		TimeoutsConfig: tConfig,
		LogsConfig:     lConfig,
	}

	return c, nil

}

// manageExperimentChannels starts up the various experimentutil.Workers and listens for their response.  It puts these
// statuses into an aggregate channel.
func manageExperimentChannels(ctx context.Context, exptConfigs []exptConfig) <-chan experimentutil.ExperimentSuccess {
	agg := make(chan experimentutil.ExperimentSuccess, len(exptConfigs))
	var wg sync.WaitGroup
	wg.Add(len(exptConfigs))

	t, ok := tConfig["exptTimeoutDuration"].(time.Duration)
	if !ok {
		log.Error("exptTimeoutDuration is not a time.Duration object")
	}

	// Start all of the experiment workers, put their results into the agg channel
	for _, eConfig := range exptConfigs {
		go func(eConfig exptConfig) {
			defer wg.Done()
			if !ok {
				return
			}
			exptContext, exptCancel := context.WithTimeout(ctx, t)
			defer exptCancel()

			// If all goes well, each experiment Worker channel will be ready to be received on twice:  once when the
			// successful status is sent, and when the channel closes after cleanup.  If we timeout, just move on.
			// Expt channel is buffered anyway, so if the worker tries to send later and there's no receiver,
			// garbage collection will take care of it
			c := experimentutil.Worker(exptContext, eConfig, log, promPush)
			select {
			case status := <-c: // Grab status from channel
				agg <- status
				<-c // Block until channel closes, which means experiment worker is done with everything
			case <-exptContext.Done():
				if err := exptContext.Err(); err == context.DeadlineExceeded {
					log.Error("Timed out waiting for experiment success info to be reported. Someone from USDC should " +
						"look into this and cleanup if needed.  See " +
						"https://cdcvs.fnal.gov/redmine/projects/discompsupp/wiki/MANAGEDPROXIES for instructions.")
				} else {
					log.Error(err)
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
	log.Info("Running script as ", cuser.Username)
	if cuser.Username != authuser {
		return fmt.Errorf("This must be run as %s.  Trying to run as %s", authuser, cuser.Username)
	}
	return nil
}
