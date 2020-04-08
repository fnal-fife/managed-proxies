package proxypush

import (
	"context"
	"fmt"
	"regexp"
	"sync"

	"github.com/jinzhu/copier"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"

	"cdcvs.fnal.gov/discompsupp/ken_proxy_push/v3/notifications"
	"cdcvs.fnal.gov/discompsupp/ken_proxy_push/v3/proxy"
	"cdcvs.fnal.gov/discompsupp/ken_proxy_push/v3/utils"
)

var (
	// EmailRegexp is a regexp that validates email addresses
	EmailRegexp = regexp.MustCompile(`^[\w\._%+-]+@[\w\.-]+\.\w{2,}$`)
)

// Funcs to manage the workflow of the proxy-push cmd

// ExperimentChannelManager starts up the various workers and listens for their response.  It puts these
// statuses into an aggregate channel.
func ExperimentChannelManager(ctx context.Context, exptConfigs []*utils.ExptConfig, globalnConfig notifications.Config, tConfig utils.TimeoutsConfig, promPush notifications.BasicPromPush) <-chan Success {
	agg := make(chan Success, len(exptConfigs))
	configChan := make(chan *utils.ExptConfig) // chan of configurations to send to workerSlots
	var wg, wwg, nwg sync.WaitGroup
	// Couple all these waitgroups so their collective status drives logic
	waitGroups := waitGroupCollection{
		&wg,  // increment this when experiment ProxyPushJobs are started
		&wwg, // increment this when worker slots are started
		&nwg, // increment this when notification.Managers are started
	}

	wg.Add(len(exptConfigs)) // Get number of experiments to run on

	// Get number of workers from config, launch workers
	wwg.Add(viper.GetInt("global.numPushWorkers"))
	log.WithField("numPushWorkers", viper.GetInt("global.numPushWorkers")).Debug("Starting workers")
	go func() {
		for i := 0; i < viper.GetInt("global.numPushWorkers"); i++ {
			go workerSlot(ctx, i, configChan, agg, &wwg, &wg, &nwg, globalnConfig, tConfig, promPush)
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

// workerSlot is a slot into which workers can be assigned.  global.numPushWorkers defines the number of these that manageExperimentChannels will create.
// Listens on configChan and writes to aggChan
func workerSlot(ctx context.Context, workerID int, configChan <-chan *utils.ExptConfig, aggChan chan<- Success, wwg, ewg, nwg *sync.WaitGroup, globalnConfig notifications.Config, tConfig utils.TimeoutsConfig, promPush notifications.BasicPromPush) {

	defer func() {
		log.WithField("workerId", workerID).Debug("Worker Slot shutting down")
		wwg.Done()
	}()

	for eConfig := range configChan {
		func(eConfig *utils.ExptConfig) {
			/* Order of operations in cleanup:
			* Experiment finishes processing, closes notification Manager and experiment success channel
			* Notifications get sent, then nwg is decremented
			* Once nwg and wg are down to 0, we close the agg channel to tell main to cleanup
			 */
			defer ewg.Done()
			exptSubConfig := viper.Sub("experiments." + eConfig.Name)

			// Notifications setup
			n := notifications.Config{}
			copier.Copy(&n, &globalnConfig)
			n.Experiment = eConfig.Name
			if !viper.GetBool("test") {
				n.To = exptSubConfig.GetStringSlice("emails")
			}
			n.Subject = n.Subject + " - " + eConfig.Name

			// Record that we've started a notification manager so any relevant goroutines that rely on all notifications being sent are blocked appropriately
			nwg.Add(1)
			nMgr := notifications.NewManager(ctx, nwg, n)

			exptContext, exptCancel := context.WithTimeout(ctx, tConfig["expttimeoutDuration"])
			defer exptCancel()

			// If all goes well, each experiment Worker channel will be ready to be received on twice:  once when the
			// successful status is sent, and when the channel closes after cleanup.  If we timeout, just move on.
			// Expt channel is buffered anyway, so if the worker tries to send later and there's no receiver,
			// garbage collection will take care of it
			log.WithFields(log.Fields{"experiment": eConfig.Name}).Debug("Starting worker")
			c := worker(exptContext, eConfig, promPush, nMgr)
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

// CheckNumWorkers makes sure there is at least one worker configured to handle the proxy pushes
func CheckNumWorkers() {
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

// Funcs to manage the actual copying of proxies

// CopyFileError is an error type that is returned when a file cannot be copied, either locally or remotely.
type CopyFileError struct{ message string }

func (c *CopyFileError) Error() string { return c.message }

type sourcePather interface {
	sourcePath() string
}

// vomsProxy is an extended and locally-used proxy.VomsProxy
type vomsProxy proxy.VomsProxy

// sourcePath gets the path where the vomsProxy is stored
func (v *vomsProxy) sourcePath() string { return v.Path }

func newWrappedVomsProxy(v *proxy.VomsProxy) *vomsProxy {
	_p := *v
	_vp := vomsProxy(_p)
	return &_vp
}

type copyToDestinationer interface {
	copyToDestination(ctx context.Context, source string) error
}

// copySourceToDestination copies a sourcePather to a copyToDestinationer, as directed in the latter.
func copySourceToDestination(ctx context.Context, s sourcePather, c copyToDestinationer) error {
	if err := c.copyToDestination(ctx, s.sourcePath()); err != nil {
		return &CopyFileError{"Could not copy source to destination"}
	}
	return nil
}
