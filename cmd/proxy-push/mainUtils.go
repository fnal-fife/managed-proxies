package main

import (
	"context"
	"errors"
	"fmt"
	"os/user"
	"regexp"
	"sync"

	"github.com/jinzhu/copier"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"

	"cdcvs.fnal.gov/discompsupp/ken_proxy_push/v3/experiment"
	"cdcvs.fnal.gov/discompsupp/ken_proxy_push/v3/notifications"
)

var emailRegexp = regexp.MustCompile(`^[\w\._%+-]+@[\w\.-]+\.\w{2,}$`)

// manageExperimentChannels starts up the various experimentutil.Workers and listens for their response.  It puts these
// statuses into an aggregate channel.
func manageExperimentChannels(ctx context.Context, exptConfigs []experiment.ExptConfig) <-chan experiment.Success {
	agg := make(chan experiment.Success, len(exptConfigs))
	configChan := make(chan experiment.ExptConfig) // chan of configurations to send to workerSlots
	var wg, wwg, nwg sync.WaitGroup
	waitGroups := waitGroupCollection{
		&wg,  // experiment workers
		&wwg, // Worker slots
		&nwg, // Notification managers
	}

	wg.Add(len(exptConfigs)) // Get number of experiments to run on

	// Get number of workers from config, launch workers
	wwg.Add(viper.GetInt("global.numPushWorkers"))
	log.WithField("numPushWorkers", viper.GetInt("global.numPushWorkers")).Debug("Starting Workers")
	go func() {
		for i := 0; i < viper.GetInt("global.numPushWorkers"); i++ {
			go workerSlot(ctx, i, configChan, agg, &wwg, &wg, &nwg)
		}
	}()

	// Put our expt configs into the configChan so workers can start processing experiments
	go func() {
		defer close(configChan)
		for _, eConfig := range exptConfigs {
			configChan <- eConfig
		}
	}()

	/* This will wait until all expt workers have put their values into agg channel, and have finished
	sending notifications.

	This prevents the 2 rare race conditions: 1) that main() returns before all expt cleanup
	is done (since main() waits for the agg channel to close before doing cleanup), and 2) we close the
	agg channel before all values have been sent into it.
	*/

	// Wait for the previous two goroutines to finish and close agg
	go func() {
		waitGroups.Wait()
		log.Debug("Closing aggregation channel")
		close(agg)
	}()

	return agg
}

// workerSlot is a slot into which experiment.Workers can be assigned.  global.numPushWorkers defines the number of these that manageExperimentChannels will create.
// Listens on configChan and writes to aggChan
func workerSlot(ctx context.Context, workerID int, configChan <-chan experiment.ExptConfig, aggChan chan<- experiment.Success, wwg, ewg, nwg *sync.WaitGroup) {

	defer func() {
		log.WithField("workerId", workerID).Debug("Worker Slot shutting down")
		wwg.Done()
	}()

	for eConfig := range configChan {
		func(eConfig experiment.ExptConfig) {
			/* Order of operations in cleanup:
			* Experiment finishes processing, closes notification Manager and experiment success channel
			* Notifications get sent, then nwg is decremented
			* Once nwg and wg are down to 0, we close the agg channel to tell main to cleanup
			 */
			defer ewg.Done()
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
			nMgr := notifications.NewManager(ctx, nwg, n)

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
					aggChan <- status
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
		vomsprefix = viper.GetString("global.defaultvomsprefixroot") + expt + "/"
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
		IsTest:         viper.GetBool("test"),
	}

	log.WithField("experiment", c.Name).Debug("Set up experiment config")
	return c, nil

}

// checkUser verifies that the current user is the authorized user to run this executable
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

// checkNumWorkers makes sure there is at least one worker configured to handle the proxy pushes
func checkNumWorkers() {
	if viper.GetInt("global.numpushworkers") < 1 {
		msg := fmt.Sprintf("Must have at least 1 Proxy Push Worker Slot.  The current number configured is %d", viper.GetInt("global.numpushworkers"))
		log.Panic(msg)
	}
}

// waitGroupCollection is a slice of pointers to WaitGroups.  The main reason I created this was for the waitGroupCollection.Wait() method
type waitGroupCollection []*sync.WaitGroup

// Add adds a waitgroup pointer to the waitGroupCollection
func (w *waitGroupCollection) Add(wg *sync.WaitGroup) {
	*w = append(*w, wg)
}

// Wait will wait until all of the waitgroups in the waitGroupCollection have decremented to 0, and then return
func (w *waitGroupCollection) Wait() {
	var masterWg sync.WaitGroup
	masterWg.Add(len(*w))

	for _, wg := range *w {
		go func(myWaitGroup *sync.WaitGroup) {
			defer masterWg.Done()
			myWaitGroup.Wait()
		}(wg)
	}

	masterWg.Wait()
}
