package main

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"os/user"
	"strings"
	"sync"
	"time"

	"fnal.gov/sbhat/proxypush/experimentutil"
	"fnal.gov/sbhat/proxypush/notifications"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
	"github.com/rifflock/lfshook"
	"github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

const configFile string = "proxy_push_config_test.yml" // CHANGE ME BEFORE PRODUCTION

var (
	log      = logrus.New() // Global logger
	promPush notifications.BasicPromPush
	start    time.Time
)

// checkUser makes sure that the user running the proxy push is the authorized user
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

// manageExperimentChannels starts up the various experimentutil.Workers and listens for their response.  It puts these
// statuses into an aggregate channel.
func manageExperimentChannels(ctx context.Context, exptList []string) <-chan experimentutil.ExperimentSuccess {
	agg := make(chan experimentutil.ExperimentSuccess, len(exptList))
	var wg sync.WaitGroup
	wg.Add(len(exptList))

	t, ok := viper.Get("exptTimeoutDuration").(time.Duration)
	if !ok {
		log.Error("exptTimeoutDuration is not a time.Duration object")
	}

	// Start all of the experiment workers, put their results into the agg channel
	for _, expt := range exptList {
		go func(expt string) {
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
			c := experimentutil.Worker(exptContext, expt, log, promPush)
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
		}(expt)
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

func init() {
	// Defaults
	viper.SetDefault("notifications.admin_email", "fife-group@fnal.gov")

	// Parse our command-line arguments
	pflag.StringP("experiment", "e", "", "Name of single experiment to push proxies")
	pflag.StringP("configfile", "c", configFile, "Specify alternate config file")
	pflag.BoolP("test", "t", false, "Test mode")

	pflag.Parse()
	viper.BindPFlags(pflag.CommandLine)

	// Read the config file
	viper.SetConfigFile(viper.GetString("configfile"))
	err := viper.ReadInConfig()
	if err != nil {
		panic(fmt.Errorf("Fatal error config file: %s", err))
	}

	// From here on out, we're logging to the log file too
	// Set up our global logger
	log.Level = logrus.DebugLevel

	// Error Log
	log.AddHook(lfshook.NewHook(lfshook.PathMap{
		logrus.ErrorLevel: viper.GetString("logs.errfile"),
		logrus.FatalLevel: viper.GetString("logs.errfile"),
		logrus.PanicLevel: viper.GetString("logs.errfile"),
	}, new(experimentutil.ExptErrorFormatter)))

	// Master Log
	log.AddHook(lfshook.NewHook(lfshook.PathMap{
		logrus.InfoLevel:  viper.GetString("logs.logfile"),
		logrus.WarnLevel:  viper.GetString("logs.logfile"),
		logrus.ErrorLevel: viper.GetString("logs.logfile"),
		logrus.FatalLevel: viper.GetString("logs.logfile"),
		logrus.PanicLevel: viper.GetString("logs.logfile"),
	}, &logrus.TextFormatter{FullTimestamp: true}))

	// Debug Log
	log.AddHook(lfshook.NewHook(lfshook.PathMap{
		logrus.DebugLevel: viper.GetString("logs.debugfile"),
		logrus.InfoLevel:  viper.GetString("logs.debugfile"),
		logrus.WarnLevel:  viper.GetString("logs.debugfile"),
		logrus.ErrorLevel: viper.GetString("logs.debugfile"),
		logrus.FatalLevel: viper.GetString("logs.debugfile"),
		logrus.PanicLevel: viper.GetString("logs.debugfile"),
	}, &logrus.TextFormatter{FullTimestamp: true}))

	log.Debugf("Using config file %s", viper.GetString("configfile"))

	// Test flag sets which notifications section from config we want to use.
	if viper.GetBool("test") {
		log.Info("Running in test mode")
		viper.Set("notifications", viper.Get("notifications_test"))
	}

	// Now that our log is set up and we've got a valid config, handle all init (fatal) errors using the following func
	// that logs the error, sends a slack message and an email, cleans up, and then exits.
	initErrorNotify := func(m string) {
		log.Error(m)

		// Durations are hard-coded here since we haven't parsed them out yet
		slackInitCtx, slackInitCancel := context.WithTimeout(context.Background(), time.Duration(15*time.Second))
		notifications.SendSlackMessage(slackInitCtx, m)
		slackInitCancel()

		emailInitCtx, emailInitCancel := context.WithTimeout(context.Background(), time.Duration(30*time.Second))
		notifications.SendEmail(emailInitCtx, "", m)
		emailInitCancel()

		if _, err := os.Stat(viper.GetString("logs.errfile")); !os.IsNotExist(err) {
			if e := os.Remove(viper.GetString("logs.errfile")); e != nil {
				log.Warn("Could not remove error file.  Please remove manually")
			}
		}
		os.Exit(1)
	}

	// Check that we're running as the right user
	cuser, err := user.Current()
	if err != nil {
		initErrorNotify("Could not lookup current user.  Exiting")
	}
	log.Info("Running script as ", cuser.Username)
	if cuser.Username != viper.GetString("global.should_runuser") {
		msg := fmt.Sprintf("This must be run as %s.  Trying to run as %s",
			viper.GetString("global.should_runuser"), cuser.Username)
		initErrorNotify(msg)
	}

	// Parse our timeouts, store them into timeoutDurationMap for later use
	for timeoutName, timeoutString := range viper.GetStringMapString("timeout") {
		value, err := time.ParseDuration(timeoutString)
		if err != nil {
			msg := fmt.Sprintf("Invalid %s value: %s", timeoutName, timeoutString)
			initErrorNotify(msg)
		}
		newName := timeoutName + "Duration"
		viper.Set(newName, value)
	}

	// Set up prometheus pusher

	promPush.R = prometheus.NewRegistry()
	promPush.P = push.New(viper.GetString("prometheus.host"), viper.GetString("prometheus.jobname")).Gatherer(promPush.R)
	if err := promPush.RegisterMetrics(); err != nil {
		log.Errorf("Error registering prometheus metrics: %s", err.Error())
	}
}

func cleanup(exptStatus map[string]bool, experiments []string) error {
	defer func() {
		if err := promPush.PushPromTotalDuration(start); err != nil {
			log.Error(err.Error())
			notifications.SendSlackMessage(context.Background(), err.Error())
		}
	}()

	s := make([]string, 0, len(experiments))
	f := make([]string, 0, len(experiments))

	// Compile list of successes and failures
	for _, expt := range experiments {
		if _, ok := exptStatus[expt]; !ok {
			f = append(f, expt)
		}
	}

	for expt, success := range exptStatus {
		if success {
			s = append(s, expt)
		} else {
			f = append(f, expt)
		}
	}

	// Defining this defer func here so we have the correct failure count
	defer func() {
		if err := promPush.PushCountErrors(len(f)); err != nil {
			log.Error(err.Error())
			notifications.SendSlackMessage(context.Background(), err.Error())
		}
	}()

	log.Infof("Successes: %v\nFailures: %v\n", strings.Join(s, ", "), strings.Join(f, ", "))

	if _, err := os.Stat(viper.GetString("logs.errfile")); os.IsNotExist(err) {
		log.Info("Proxy Push completed with no errors")
		return nil
	}

	// We have an error file, so presumably we have errors.  Read the errorfile and send notifications
	data, err := ioutil.ReadFile(viper.GetString("logs.errfile"))
	if err != nil {
		return err
	}

	finalCleanupSuccess := true
	msg := string(data)

	t, ok := viper.Get("emailTimeoutDuration").(time.Duration)
	if !ok {
		return errors.New("emailTimeoutDuration is not a time.Duration object")
	}
	emailCtx, emailCancel := context.WithTimeout(context.Background(), t)
	if err = notifications.SendEmail(emailCtx, "", msg); err != nil {
		log.Error(err)
		finalCleanupSuccess = false
	}
	emailCancel()

	t, ok = viper.Get("slackTimeoutDuration").(time.Duration)
	if !ok {
		return errors.New("slackTimeoutDuration is not a time.Duration object")
	}
	slackCtx, slackCancel := context.WithTimeout(context.Background(), t)
	if err = notifications.SendSlackMessage(slackCtx, msg); err != nil {
		log.Error(err)
		finalCleanupSuccess = false
	}
	slackCancel()

	if err = os.Remove(viper.GetString("logs.errfile")); err != nil {
		log.Error("Could not remove general error logfile.  Please clean up manually")
		finalCleanupSuccess = false
	}

	if !finalCleanupSuccess {
		return errors.New("Could not clean up.  Please review")
	}

	return nil
}

func main() {
	start = time.Now()
	exptSuccesses := make(map[string]bool)                             // map of successful expts
	expts := make([]string, 0, len(viper.GetStringMap("experiments"))) // Slice of experiments we will actually process

	// Get our list of experiments from the config file, set exptConfig Name variable
	if viper.GetString("experiment") != "" {
		// If experiment is passed in on command line
		expts = append(expts, viper.GetString("experiment"))
	} else {
		// No experiment on command line, so use all expts in config file
		for k := range viper.GetStringMap("experiments") {
			expts = append(expts, k)
		}
	}

	// Start up the expt manager
	t, ok := viper.Get("globalTimeoutDuration").(time.Duration)
	if !ok {
		log.Fatal("globalTimeoutDuration is not a time.Duration object")
	}
	ctx, cancel := context.WithTimeout(context.Background(), t)
	defer cancel()
	c := manageExperimentChannels(ctx, expts)
	// Listen on the manager channel
	for {
		select {
		case expt, chanOpen := <-c:
			// Manager channel is closed, so cleanup.
			if !chanOpen {
				err := cleanup(exptSuccesses, expts)
				if err != nil {
					log.Error(err)
				}
				return
			}
			// Otherwise, add the information coming in to the map.
			exptSuccesses[expt.Name] = expt.Success
		case <-ctx.Done():
			// Timeout
			if e := ctx.Err(); e == context.DeadlineExceeded {
				log.Error("Hit the global timeout!")
			} else {
				log.Error(e)
			}

			if err := cleanup(exptSuccesses, expts); err != nil {
				log.Error(err)
			}
			return
		}
	}
}
