package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
	"github.com/rifflock/lfshook"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	"cdcvs.fnal.gov/discompsupp/ken_proxy_push/v3/experiment"
	"cdcvs.fnal.gov/discompsupp/ken_proxy_push/v3/notifications"
	"cdcvs.fnal.gov/discompsupp/ken_proxy_push/v3/packaging"
)

const configFile string = "managedProxies"

// Sub-config types

var (
	tConfig   experiment.TimeoutsConfig
	nConfig   notifications.Config
	krbConfig experiment.KerbConfig

	promPush       notifications.BasicPromPush
	prometheusUp   = true
	startSetup     time.Time
	startProxyPush time.Time
	startCleanup   time.Time
)

func init() {
	var nKey string
	startSetup = time.Now()
	// Defaults
	viper.SetDefault("notifications.admin_email", "fife-group@fnal.gov")
	viper.SetDefault("global.numpushworkers", 10)

	// Parse our command-line arguments
	pflag.StringP("experiment", "e", "", "Name of single experiment to push proxies")
	pflag.StringP("configfile", "c", "", "Specify alternate config file")
	pflag.BoolP("test", "t", false, "Test mode")
	pflag.Bool("version", false, "Version of Managed Proxies library")
	pflag.String("admin", "", "Override the config file admin email")

	pflag.Parse()
	viper.BindPFlags(pflag.CommandLine)

	if viper.GetBool("version") {
		fmt.Printf("Managed Proxies Version %s, Build %s\n", packaging.Version, packaging.Build)
		os.Exit(0)
	}

	if viper.GetString("admin") != "" {
		if !emailRegexp.MatchString(viper.GetString("admin")) {
			fmt.Printf("Admin email address %s is invalid!  It must follow the regexp %s\n", viper.GetString("admin"), emailRegexp.String())
			os.Exit(1)
		}
	}

	// Read the config file
	if viper.GetString("configfile") != "" {
		viper.SetConfigFile(viper.GetString("configfile"))
	} else {
		viper.SetConfigName(configFile)
	}

	viper.AddConfigPath("/etc/managed-proxies/")
	viper.AddConfigPath("$HOME/managed-proxies/")
	viper.AddConfigPath(".")
	if err := viper.ReadInConfig(); err != nil {
		panic(fmt.Errorf("Fatal error config file: %s", err))
	}

	log.SetLevel(log.DebugLevel)
	// Set up logs
	log.AddHook(lfshook.NewHook(lfshook.PathMap{
		log.DebugLevel: viper.GetString("logs.debugfile"),
		log.InfoLevel:  viper.GetString("logs.debugfile"),
		log.WarnLevel:  viper.GetString("logs.debugfile"),
		log.ErrorLevel: viper.GetString("logs.debugfile"),
		log.FatalLevel: viper.GetString("logs.debugfile"),
		log.PanicLevel: viper.GetString("logs.debugfile"),
	}, &log.TextFormatter{FullTimestamp: true}))

	log.AddHook(lfshook.NewHook(lfshook.PathMap{
		log.InfoLevel:  viper.GetString("logs.logfile"),
		log.WarnLevel:  viper.GetString("logs.logfile"),
		log.ErrorLevel: viper.GetString("logs.logfile"),
		log.FatalLevel: viper.GetString("logs.logfile"),
		log.PanicLevel: viper.GetString("logs.logfile"),
	}, &log.TextFormatter{FullTimestamp: true}))

	// Make sure we have a reasonable amount of workers
	checkNumWorkers()

	log.Debugf("Using config file %s", viper.ConfigFileUsed())

	// Set up notifications
	nConfig.ConfigInfo = make(map[string]string)
	nKey = "notifications"
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
	nConfig.IsTest = viper.GetBool("test")
	timestamp := time.Now().Format(time.RFC822)
	nConfig.Subject = fmt.Sprintf("Managed Proxy Service Errors - Proxy Push - %s", timestamp)
	setAdminEmail(&nConfig)

	// Now that our log is set up and we've got a valid config, handle all init (fatal) errors using the following func
	// that logs the error, sends a slack message and an email, cleans up, and then exits.
	initErrorNotify := func(m string) {
		log.WithFields(log.Fields{"caller": "main.init"}).Error(m)
		nConfig.Subject = "Error setting up proxy-push"

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
	if err := checkUser(viper.GetString("global.authuser")); err != nil {
		initErrorNotify(err.Error())
	}

	// Parse our timeouts, store them into timeoutDurationMap for later use
	tConfig = make(experiment.TimeoutsConfig)

	for timeoutName, timeoutString := range viper.GetStringMapString("times") {
		value, err := time.ParseDuration(timeoutString)
		if err != nil {
			msg := fmt.Sprintf("Invalid %s value: %s", timeoutName, timeoutString)
			initErrorNotify(msg)
		}
		newName := timeoutName + "Duration"
		tConfig[newName] = value
	}

	krbConfig = make(experiment.KerbConfig)
	for key, value := range viper.GetStringMapString("kerberos") {
		krbConfig[key] = value
	}

	log.WithFields(log.Fields{"caller": "main.init"}).Debug("Read in config file to config structs")

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

func cleanup(exptStatus map[string]bool, expts []string) error {
	// Since cleanup happens in all cases after the proxy push starts, we stop that timer and push the metric here
	if viper.GetString("experiment") == "" && !viper.GetBool("test") {
		// Only push this metric if we ran for all experiments to keep data consistent
		if err := promPush.PushPromDuration(startProxyPush, "proxy-push", "processing"); err != nil {
			msg := "Error recording time to push proxies, " + err.Error()
			log.Error(msg)
		}
	}

	s := make([]string, 0, len(expts))
	f := make([]string, 0, len(expts))

	// Compile list of successes and failures
	// If there's any experiment for which we didn't receive a status, it's a failure
	for _, e := range expts {
		if _, ok := exptStatus[e]; !ok {
			f = append(f, e)
		}
	}

	for expt, success := range exptStatus {
		if success {
			s = append(s, expt)
		} else {
			f = append(f, expt)
		}
	}

	defer log.Infof("Successes: %v\nFailures: %v\n", strings.Join(s, ", "), strings.Join(f, ", "))

	if viper.GetBool("test") {
		log.Infof("Running in test mode.  Will not push metrics to prometheus")
		return nil
	}

	// Defining this defer func here so we have the correct failure count
	if err := promPush.PushCountErrors(len(f)); err != nil {
		log.Error(err.Error())
		notifications.SendSlackMessage(context.Background(), nConfig, err.Error())
	}

	return nil
}

func main() {
	var nwg sync.WaitGroup
	exptSuccesses := make(map[string]bool) // map of successful expts
	ctx, cancel := context.WithTimeout(context.Background(), tConfig["globaltimeoutDuration"])

	/* Order of operations
	1. Set up experiment configs
	2. Start up experiment managers
	3. Wait for successes (wg?)
	4.  Tally up successes/failures (and DON'T move forward until all experiments are done!  They will send own notifications)
	4. Cleanup (including summarizing failures.  Keep that here.  No need for separate function)
	*/

	/* Order of defers (in execution order)
	* Close notification manager
	* Wait on notification waitgroup
	* Push prometheus timestamp for processing
	* Send admin notifications
	* Cancel global context
	 */
	defer cancel()

	// Start a notifications manager for the admin notifications.  This will run separately from the experiment-specific managers
	nwg.Add(1)
	defer nwg.Wait()
	nMgr := notifications.NewManager(ctx, &nwg, nConfig)
	defer close(nMgr)

	// Send admin notifications at the end
	defer func() {
		if err := notifications.SendAdminNotifications(ctx, nConfig, "proxy-push"); err != nil {
			log.WithField("caller", "main").Error("Error sending Admin Notifications")
		}
	}()

	exptConfigs := make([]experiment.ExptConfig, 0, len(viper.GetStringMap("experiments"))) // Slice of experiment configurations we will actually process
	expts := make([]string, 0, len(exptConfigs))

	// Get our list of experiments from the config file, create exptConfig objects
	if viper.GetString("experiment") != "" {
		// If experiment is passed in on command line
		eConfig, err := createExptConfig(viper.GetString("experiment"))
		if err != nil {
			log.WithFields(log.Fields{
				"experiment": viper.GetString("experiment"),
				"caller":     "main",
			}).Error("Error setting up experiment configuration slice.  As this is the only experiment, we will cleanup now.")
			os.Exit(1)
		}
		exptConfigs = append(exptConfigs, eConfig)
		expts = append(expts, eConfig.Name)
	} else {
		// No experiment on command line, so use all expts in config file
		for k := range viper.GetStringMap("experiments") {
			eConfig, err := createExptConfig(k)
			if err != nil {
				log.WithFields(log.Fields{
					"experiment": k,
					"caller":     "main",
				}).Error("Error setting up experiment configuration slice")
			}
			exptConfigs = append(exptConfigs, eConfig)
			expts = append(expts, eConfig.Name)
		}
	}

	// Setup is done here.  Push the time
	if err := promPush.PushPromDuration(startSetup, "proxy-push", "setup"); err != nil {
		log.WithFields(log.Fields{"caller": "main"}).Errorf("Error recording time to setup, %s", err.Error())
	}

	startProxyPush = time.Now()
	// Start up the expt manager
	c := manageExperimentChannels(ctx, exptConfigs)
	// Listen on the manager channel
	for {
		select {
		case expt, chanOpen := <-c:
			// Manager channel is closed, so cleanup.
			if !chanOpen {
				err := cleanup(exptSuccesses, expts)
				if err != nil {
					log.WithFields(log.Fields{"caller": "main"}).Error("Unable to cleanup")
				}
				return
			}
			// Otherwise, add the information coming in to the map.
			exptSuccesses[expt.Name] = expt.Successful
		case <-ctx.Done():
			// Timeout
			if e := ctx.Err(); e == context.DeadlineExceeded {
				log.WithFields(log.Fields{"caller": "main"}).Error("Hit the global timeout!")
			} else {
				log.WithFields(log.Fields{"caller": "main"}).Error(e)
			}
			if err := cleanup(exptSuccesses, expts); err != nil {
				log.WithFields(log.Fields{"caller": "main"}).Error("Unable to cleanup")
			}
			return
		}
	}
}
