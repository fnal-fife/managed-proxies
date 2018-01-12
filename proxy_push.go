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
	"github.com/rifflock/lfshook"
	"github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

// Error handling - break everything!
// Email stuff needs to be called from here, though it's also in expt utils
// Timeouts to config file

// Timeouts, defaults, and format strings

const (
	configFile      string = "proxy_push_config_test.yml"       // CHANGE ME BEFORE PRODUCTION
	exptLogFilename string = "golang_proxy_push_%s.log"         // CHANGE ME BEFORE PRODUCTION - temp file per experiment that will be emailed to experiment
	exptGenFilename string = "golang_proxy_push_general_%s.log" // CHANGE ME BEFORE PRODUCTION - temp file per experiment that will be copied over to logfile
)

// var timeoutStrings = map[string]string{
// 						"globalTimeout": "60s", // Global timeout
// 						"exptTimeout":   "30s", // Experiment timeout
// 						"pingTimeout":   "10s", // Ping timeout (for all pings per experiment)
// 						"vpiTimeout":    "10s", // voms-proxy-init timeout (total for vpis per experiment)
// 						"copyTimeout":   "30s", // copy proxy timeout (total for all copies per experiment)
// 						"slackTimeout":  "15s", // Slack message timeout
// 						"emailTimeout":  "30s"} // Email timeout (per email)
// var timeoutDurationMap map[string]time.Duration // Hold the durations for code to use

var log = logrus.New() // Global logger
// var rwmuxErr, rwmuxLog sync.RWMutex                          // mutexes to be used when copying experiment logs into master and error log
// if testMode is true:  send only general emails to notifications_test.admin_email in config file, send Slack notification to test channel. Do not send experiment-specific email
// var testMode bool

// // experimentutil.ExperimentSuccess stores information on whether all the processes involved in generating, copying, and changing permissions on all proxies for
// // an experiment were successful.
// type experimentutil.ExperimentSuccess struct {
// 	name    string
// 	success bool
// }

// Global functions

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

// manageExperimentChannels starts up the various ExperimentWorkers and listens for their response.  It puts these statuses into an aggregate channel.
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

			// If all goes well, each ExperimentWorker channel will be ready to be received on twice:  once when the
			// successful status is sent, and when the channel closes after cleanup.  If we timeout, just move on.  Expt channel
			// is buffered anyway, so if the worker tries to send later and there's no receiver, garbage collection
			// will take care of it
			c := experimentutil.ExperimentWorker(exptContext, expt, log)
			select {
			case status := <-c: // Grab status from channel
				agg <- status
				<-c // Block until channel closes, which means experiment worker is done with everything
			case <-exptContext.Done():
				if err := exptContext.Err(); err == context.DeadlineExceeded {
					log.Error("Timed out waiting for experiment success info to be reported")
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

// sendEmail sends emails to both experiments and admins, depending on the input (exptName = "" gives admin email).
// func sendEmail(ctx context.Context, exptName, message string) error {
// var recipients []string

// if exptName == "" {
// 	exptName = "all experiments" // Send email for all experiments to admin
// 	recipients = viper.GetStringSlice("notifications.admin_email")
// } else {
// 	emailsKeyLookup := "experiments." + exptName + ".emails"
// 	recipients = viper.GetStringSlice(emailsKeyLookup)
// }

// subject := "Managed Proxy Push errors for " + exptName

// m := gomail.NewMessage()
// m.SetHeader("From", "fife-group@fnal.gov")
// m.SetHeader("To", recipients...)
// m.SetHeader("Subject", subject)
// m.SetBody("text/plain", message)

// c := make(chan error)
// go func() {
// 	defer close(c)
// 	err := emailDialer.DialAndSend(m)
// 	c <- err
// }()

// select {
// case e := <-c:
// 	return e
// case <-ctx.Done():
// 	e := ctx.Err()
// 	if e != context.DeadlineExceeded {
// 		return fmt.Errorf("Hit timeout attempting to send email to %s", exptName)
// 	}
// 	return e
// }
// }

// // sendSlackMessage sends an HTTP POST request to a URL specified in the config file.
// func sendSlackMessage(ctx context.Context, message string) error {
// 	if e := ctx.Err(); e != nil {
// 		return e
// 	}

// 	msg := []byte(fmt.Sprintf(`{"text": "%s"}`, strings.Replace(message, "\"", "\\\"", -1)))
// 	req, err := http.NewRequest("POST", viper.GetString("notifications.slack_alerts_url"), bytes.NewBuffer(msg))
// 	if err != nil {
// 		return err
// 	}

// 	req.Header.Set("Content-Type", "application/json")

// 	// Actually send the request
// 	client := &http.Client{Timeout: viper.Get("slackTimeoutDuration")}
// 	resp, err := client.Do(req)
// 	if err != nil {
// 		return err
// 	}

// 	// This should be redundant, but just in case the timeout before didn't trigger.
// 	if e := ctx.Err(); e != nil {
// 		return e
// 	}

// 	defer resp.Body.Close()

// 	// Parse the response to make sure we're good
// 	if resp.StatusCode != http.StatusOK {
// 		body, _ := ioutil.ReadAll(resp.Body)
// 		errmsg := fmt.Errorf("Slack Response Status: %s\nSlack Response Headers: %s\nSlack Response Body: %s",
// 			resp.Status, resp.Header, string(body))
// 		return errmsg
// 	}
// 	fmt.Println("Slack message sent")
// 	return nil
// }

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
	logFormatter := logrus.TextFormatter{FullTimestamp: true}
	log.Formatter = &logFormatter

	// Error log
	log.AddHook(lfshook.NewHook(lfshook.PathMap{
		logrus.ErrorLevel: viper.GetString("logs.errfile"),
		logrus.FatalLevel: viper.GetString("logs.errfile"),
		logrus.PanicLevel: viper.GetString("logs.errfile"),
	}))

	// Master Log
	log.AddHook(lfshook.NewHook(lfshook.PathMap{
		logrus.DebugLevel: viper.GetString("logs.logfile"),
		logrus.InfoLevel:  viper.GetString("logs.logfile"),
		logrus.WarnLevel:  viper.GetString("logs.logfile"),
		logrus.ErrorLevel: viper.GetString("logs.logfile"),
		logrus.FatalLevel: viper.GetString("logs.logfile"),
		logrus.PanicLevel: viper.GetString("logs.logfile"),
	}))

	log.Debugf("Using config file %s", viper.GetString("configfile"))

	// Test flag sets which notifications section from config we want to use.
	// testMode =
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
		msg := fmt.Sprintf("This must be run as %s.  Trying to run as %s", viper.GetString("global.should_runuser"), cuser.Username)
		initErrorNotify(msg)
	}

	// Parse our timeouts, store them into timeoutDurationMap for later use
	// timeoutDurationMap = make(map[string]time.Duration)
	for timeoutName, timeoutString := range viper.GetStringMapString("timeout") {
		value, err := time.ParseDuration(timeoutString)
		if err != nil {
			msg := fmt.Sprintf("Invalid %s value: %s", timeoutName, timeoutString)
			initErrorNotify(msg)
		}
		newName := timeoutName + "Duration"
		viper.Set(newName, value)
		// timeoutDurationMap[timeoutName] = value
	}
}

func cleanup(exptStatus map[string]bool, experiments []string) error {
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
	exptSuccesses := make(map[string]bool)                             // map of successful expts
	expts := make([]string, 0, len(viper.GetStringMap("experiments"))) // Slice of experiments we will actually process

	// Get our list of experiments from the config file, set exptConfig Name variable
	if viper.GetString("experiment") != "" {
		expts = append(expts, viper.GetString("experiment"))
	} else {
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
