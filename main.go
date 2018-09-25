package main

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"cdcvs.fnal.gov/discompsupp/ken_proxy_push/experimentutil"
	"cdcvs.fnal.gov/discompsupp/ken_proxy_push/notifications"
	"cdcvs.fnal.gov/discompsupp/ken_proxy_push/proxyPushLogger"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
	"github.com/rifflock/lfshook"
	"github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

const configFile string = "proxy_push.yml"

// Sub-config types

var (
	log            = logrus.NewEntry(logrus.New()) // Global logger
	promPush       notifications.BasicPromPush
	prometheusUp   = true
	startSetup     time.Time
	startProxyPush time.Time
	startCleanup   time.Time
	tConfig        experimentutil.TimeoutsConfig
	nConfig        notifications.Config
	lConfig        proxyPushLogger.LogsConfig
	vConfig        experimentutil.VPIConfig
	krbConfig      experimentutil.KerbConfig
	pConfig        experimentutil.PingConfig
	sConfig        experimentutil.SSHConfig
)

func init() {
	startSetup = time.Now()
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

	// Set up the logConfig to pass to other packages
	lConfig = make(proxyPushLogger.LogsConfig)
	for key, value := range viper.GetStringMapString("logs") {
		lConfig[key] = value
	}

	log = proxyPushLogger.New("", lConfig)

	log.Debugf("Using config file %s", viper.GetString("configfile"))

	// Set up notifications
	nConfig.ConfigInfo = make(map[string]string)
	nKey := "notifications"
	// Test flag sets which notifications section from config we want to use.
	if viper.GetBool("test") {
		log.Info("Running in test mode")
		nKey = "notifications_test"
	}
	for key, value := range viper.GetStringMapString(nKey) {
		nConfig.ConfigInfo[key] = value
	}
	nConfig.ConfigInfo["smtphost"] = viper.GetString("global.smtphost")
	nConfig.ConfigInfo["smtpport"] = strconv.Itoa(viper.GetInt("global.smtpport"))
	setAdminEmail(&nConfig)
	nConfig.Logger = log

	// Now that our log is set up and we've got a valid config, handle all init (fatal) errors using the following func
	// that logs the error, sends a slack message and an email, cleans up, and then exits.
	initErrorNotify := func(m string) {
		log.WithFields(logrus.Fields{"caller": "main.init"}).Error(m)
		nConfig.Subject = "Error setting up proxy push"

		// Durations are hard-coded here since we haven't parsed them out yet
		slackInitCtx, slackInitCancel := context.WithTimeout(context.Background(), time.Duration(15*time.Second))
		notifications.SendSlackMessage(slackInitCtx, nConfig, m)
		slackInitCancel()

		emailInitCtx, emailInitCancel := context.WithTimeout(context.Background(), time.Duration(30*time.Second))
		notifications.SendEmail(emailInitCtx, nConfig, m)
		emailInitCancel()

		if _, err := os.Stat(viper.GetString("logs.errfile")); !os.IsNotExist(err) {
			if e := os.Remove(viper.GetString("logs.errfile")); e != nil {
				log.Warn("Could not remove error file.  Please remove manually")
			}
		}
		os.Exit(1)
	}

	// Check that we're running as the right user
	if err = checkUser(viper.GetString("global.should_runuser")); err != nil {
		initErrorNotify(err.Error())
	}

	// Parse our timeouts, store them into timeoutDurationMap for later use
	tConfig = make(experimentutil.TimeoutsConfig)

	for timeoutName, timeoutString := range viper.GetStringMapString("timeout") {
		value, err := time.ParseDuration(timeoutString)
		if err != nil {
			msg := fmt.Sprintf("Invalid %s value: %s", timeoutName, timeoutString)
			initErrorNotify(msg)
		}
		newName := timeoutName + "Duration"
		tConfig[newName] = value
	}

	// Set up voms-proxy-init config object
	vConfig = make(experimentutil.VPIConfig)
	for key, value := range viper.GetStringMapString("vomsproxyinit") {
		if key != "defaultvomsprefixroot" {
			vConfig[key] = value
		}
	}

	// Set up kerb config object
	krbConfig = make(experimentutil.KerbConfig)
	for key, value := range viper.GetStringMapString("kerberos") {
		krbConfig[key] = value
	}

	// Set up ping config object
	pConfig = make(experimentutil.PingConfig)
	for key, value := range viper.GetStringMapString("ping") {
		pConfig[key] = value
	}

	// Set up ssh config object
	sConfig = make(experimentutil.SSHConfig)
	for key, value := range viper.GetStringMapString("ssh") {
		sConfig[key] = value
	}

	log.WithFields(logrus.Fields{"caller": "main.init"}).Debug("Read in config file to config structs")

	// Set up prometheus pusher

	if _, err := http.Get(viper.GetString("prometheus.host")); err != nil {
		log.Errorf("Error contacting prometheus pushgateway %s: %s.  The rest of prometheus operations will fail. "+
			"To limit error noise, "+
			"these failures at the experiment level will be registered as warnings in the log, "+
			"and not be sent in any notifications.", viper.GetString("prometheus.host"), err.Error())
		prometheusUp = false
	}

	promPush.R = prometheus.NewRegistry()
	promPush.P = push.New(viper.GetString("prometheus.host"), viper.GetString("prometheus.jobname")).Gatherer(promPush.R)
	if err := promPush.RegisterMetrics(); err != nil {
		log.Errorf("Error registering prometheus metrics: %s.  Subsequent pushes will fail.  To limit error noise, "+
			"these failures at the experiment level will be registered as warnings in the log, "+
			"and not be sent in any notifications.", err.Error())
		prometheusUp = false
	}
}

func cleanup(exptStatus map[string]bool, exptConfigs []experimentutil.ExptConfig) error {
	// Since cleanup happens in all cases after the proxy push starts, we stop that timer and push the metric here
	if viper.GetString("experiment") == "" {
		// Only push this metric if we ran for all experiments to keep data consistent
		if err := promPush.PushPromDuration(startProxyPush, "proxypush"); err != nil {
			msg := "Error recording time to push proxies, " + err.Error()
			log.Error(msg)
		}
	}

	startCleanup = time.Now()

	// Logger to use to log errors during cleanup _after_ the error file has been deleted
	finalCleanupLogErr := logrus.New() // One-use logrus instance to log an error to the general file
	finalCleanupLogErr.AddHook(lfshook.NewHook(lfshook.PathMap{
		logrus.ErrorLevel: viper.GetString("logs.logfile"),
	}, &logrus.TextFormatter{FullTimestamp: true}))

	defer func() {
		if viper.GetString("experiment") == "" {
			// Only push this metric if we ran for all experiments to keep data consistent
			if err := promPush.PushPromDuration(startCleanup, "cleanup"); err != nil {
				msg := "Error recording time to cleanup, " + err.Error()
				finalCleanupLogErr.Error(msg)
				notifications.SendSlackMessage(context.Background(), nConfig, msg)
			}
		}
	}()

	s := make([]string, 0, len(exptConfigs))
	f := make([]string, 0, len(exptConfigs))

	// Compile list of successes and failures
	for _, e := range exptConfigs {
		if _, ok := exptStatus[e.Name]; !ok {
			f = append(f, e.Name)
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
			finalCleanupLogErr.Error(err.Error())
			notifications.SendSlackMessage(context.Background(), nConfig, err.Error())
		}
	}()

	log.Infof("Successes: %v\nFailures: %v\n", strings.Join(s, ", "), strings.Join(f, ", "))

	if _, err := os.Stat(viper.GetString("logs.errfile")); os.IsNotExist(err) {
		log.Info("Proxy Push completed with no errors")
		if viper.GetBool("test") {
			slackCtx, slackCancel := context.WithTimeout(context.Background(), tConfig["slacktimeoutDuration"])
			msg := "Proxies were pushed in test mode for all tested experiments successfully."
			if err = notifications.SendSlackMessage(slackCtx, nConfig, msg); err != nil {
				log.WithFields(logrus.Fields{"caller": "main.cleanup"}).Error("Error sending slack message")
			}
			slackCancel()
		}
		return nil
	}

	// We have an error file, so presumably we have errors.  Read the errorfile and send notifications
	data, err := ioutil.ReadFile(viper.GetString("logs.errfile"))
	if err != nil {
		return err
	}

	finalCleanupSuccess := true
	msg := string(data)

	setAdminEmail(&nConfig)
	nConfig.Subject = "Managed Proxy Service Errors for all experiments"
	emailCtx, emailCancel := context.WithTimeout(context.Background(), tConfig["emailtimeoutDuration"])
	if err = notifications.SendEmail(emailCtx, nConfig, msg); err != nil {
		log.WithFields(logrus.Fields{"caller": "main.cleanup"}).Error("Error sending email")
		finalCleanupSuccess = false
	}
	emailCancel()

	slackCtx, slackCancel := context.WithTimeout(context.Background(), tConfig["slacktimeoutDuration"])
	if err = notifications.SendSlackMessage(slackCtx, nConfig, msg); err != nil {
		log.WithFields(logrus.Fields{"caller": "main.cleanup"}).Error("Error sending slack message")
		finalCleanupSuccess = false
	}
	slackCancel()

	if err = os.Remove(viper.GetString("logs.errfile")); err != nil {
		log.WithFields(logrus.Fields{"caller": "main.cleanup"}).Error("Could not remove general error logfile.  Please clean up manually")
		finalCleanupSuccess = false
	}
	log.WithFields(logrus.Fields{
		"caller":   "main.cleanup",
		"filename": viper.GetString("logs.errfile"),
	}).Debug("Removed general error logfile")

	if !finalCleanupSuccess {
		return errors.New("Could not clean up completely.  Please review")
	}

	return nil
}

func main() {
	exptSuccesses := make(map[string]bool)                                                      // map of successful expts
	exptConfigs := make([]experimentutil.ExptConfig, 0, len(viper.GetStringMap("experiments"))) // Slice of experiments we will actually process

	// Get our list of experiments from the config file, create exptConfig objects
	if viper.GetString("experiment") != "" {
		// If experiment is passed in on command line
		eConfig, err := createExptConfig(viper.GetString("experiment"))
		if err != nil {
			log.WithFields(logrus.Fields{
				"experiment": viper.GetString("experiment"),
				"caller":     "main",
			}).Error("Error setting up experiment configuration slice.  As this is the only experiment, we will cleanup now.")
			if err := cleanup(exptSuccesses, exptConfigs); err != nil {
				log.WithFields(logrus.Fields{"caller": "main"}).Error("Unable to cleanup")
			}
			os.Exit(1)
		}
		exptConfigs = append(exptConfigs, eConfig)
	} else {
		// No experiment on command line, so use all expts in config file
		for k := range viper.GetStringMap("experiments") {
			eConfig, err := createExptConfig(k)
			if err != nil {
				log.WithFields(logrus.Fields{
					"experiment": k,
					"caller":     "main",
				}).Error("Error setting up experiment configuration slice")
			}
			exptConfigs = append(exptConfigs, eConfig)
		}
	}

	// Setup is done here.  Push the time
	if err := promPush.PushPromDuration(startSetup, "setup"); err != nil {
		log.WithFields(logrus.Fields{"caller": "main"}).Errorf("Error recording time to setup, %s", err.Error())
	}

	startProxyPush = time.Now()
	// Start up the expt manager
	ctx, cancel := context.WithTimeout(context.Background(), tConfig["globaltimeoutDuration"])
	defer cancel()
	c := manageExperimentChannels(ctx, exptConfigs)
	// Listen on the manager channel
	for {
		select {
		case expt, chanOpen := <-c:
			// Manager channel is closed, so cleanup.
			if !chanOpen {
				err := cleanup(exptSuccesses, exptConfigs)
				if err != nil {
					log.WithFields(logrus.Fields{"caller": "main"}).Error("Unable to cleanup")
				}
				return
			}
			// Otherwise, add the information coming in to the map.
			exptSuccesses[expt.Name] = expt.Success
		case <-ctx.Done():
			// Timeout
			if e := ctx.Err(); e == context.DeadlineExceeded {
				log.WithFields(logrus.Fields{"caller": "main"}).Error("Hit the global timeout!")
			} else {
				log.WithFields(logrus.Fields{"caller": "main"}).Error(e)
			}
			if err := cleanup(exptSuccesses, exptConfigs); err != nil {
				log.WithFields(logrus.Fields{"caller": "main"}).Error("Unable to cleanup")
			}
			return
		}
	}
}
