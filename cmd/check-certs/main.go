package main

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"os"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
	"github.com/rifflock/lfshook"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	"cdcvs.fnal.gov/discompsupp/ken_proxy_push/experiment"
	"cdcvs.fnal.gov/discompsupp/ken_proxy_push/notifications"
	"cdcvs.fnal.gov/discompsupp/ken_proxy_push/proxy"
)

const configFile string = "managedProxies.yml"

var (
	nConfig   notifications.Config
	tConfig   map[string]time.Duration
	krbConfig experiment.KerbConfig

	startSetup      time.Time
	startProcessing time.Time
	prometheusUp    bool
	promPush        notifications.BasicPromPush
)

func init() {
	var nKey string
	startSetup = time.Now()

	viper.SetDefault("notifications.admin_email", "fife-group@fnal.gov")

	pflag.StringP("configfile", "c", configFile, "Specify alternate config file")
	pflag.BoolP("test", "t", false, "Test mode (no email sent)")

	pflag.Parse()
	viper.BindPFlags(pflag.CommandLine)

	// Read the config file
	viper.SetConfigFile(viper.GetString("configfile"))
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

	log.Debugf("Using config file %s", viper.GetString("configfile"))

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
	nConfig.From = viper.GetString("notifications.admin_email")
	timestamp := time.Now().Format(time.RFC822)
	nConfig.Subject = fmt.Sprintf("Managed Proxy Service - check-certs report %s", timestamp)

	setAdminEmail(&nConfig)

	// Now that our log is set up and we've got a valid config, handle all init (fatal) errors using the following func
	// that logs the error, sends a slack message and an email, cleans up, and then exits.
	initErrorNotify := func(m string) {
		log.WithFields(log.Fields{"caller": "main.init"}).Error(m)
		nConfig.Subject = "Error setting up checkcerts"

		// Durations are hard-coded here since we haven't parsed them out yet
		slackInitCtx, slackInitCancel := context.WithTimeout(context.Background(), time.Duration(15*time.Second))
		defer slackInitCancel()
		notifications.SendSlackMessage(slackInitCtx, nConfig, m)
		os.Exit(1)
	}

	// Check that we're running as the right user
	if err := checkUser(viper.GetString("global.authuser")); err != nil {
		initErrorNotify(err.Error())
	}

	// Parse our timeouts, store them into timeoutDurationMap for later use
	tConfig = make(map[string]time.Duration)

	for timeoutName, timeoutString := range viper.GetStringMapString("times") {
		value, err := time.ParseDuration(timeoutString)
		if err != nil {
			msg := fmt.Sprintf("Invalid %s value: %s", timeoutName, timeoutString)
			initErrorNotify(msg)
		}
		newName := timeoutName + "Duration"
		tConfig[newName] = value
	}

	// Kerberos config
	krbConfig := make(experiment.KerbConfig)
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

func main() {
	var nwg, wg sync.WaitGroup
	var existsExpiringCerts bool
	certExpiration := make(map[string]time.Time)
	ctx, cancel := context.WithTimeout(context.Background(), tConfig["globaltimeoutDuration"])

	/* Order of defers (in execution order):
	* Close notification manager
	* Wait on notification waitgroup
	* Push prometheus timestamp
	* Send admin notifications
	* Cancel global context
	 */
	defer cancel()

	// Start notifications manager, just for admin

	// Send admin notifications at the end
	defer func() {
		// TODO:  if !existsExpiringCerts, set template to "all is well", send in first expiring cert.  Otherwiset template to "ALARMS" - and all certs should be in there as messages already, ".  This is in nConfig
		// TODO  Change subject in nConfig
		// If we're in test mode, don't actually send the email
		if viper.GetBool("test") {
			log.Info("Test mode.  Stopping here")
			return
		}
		if err := notifications.SendAdminNotifications(ctx, nConfig, "check-certs"); err != nil {
			log.WithField("caller", "main").Error("Error sending Admin Notifications")
		}
	}()

	nwg.Add(1)
	defer nwg.Wait()
	nMgr := notifications.NewManager(ctx, &nwg, nConfig)
	defer close(nMgr)

	// Get list of experiments
	exptConfigs := make([]experiment.ExptConfig, 0, len(viper.GetStringMap("experiments"))) // Slice of experiment configurations
	for k := range viper.GetStringMap("experiments") {
		eConfig, err := createExptConfig(k)
		if err != nil {
			log.WithFields(log.Fields{
				"experiment": k,
				"caller":     "main",
			}).Error("Error setting up experiment configuration slice")
		}
		exptConfigs = append(exptConfigs, eConfig)
	}

	// Setup is done here.  Push the time
	if err := promPush.PushPromDuration(startSetup, "check-certs", "setup"); err != nil {
		log.WithFields(log.Fields{"caller": "main"}).Errorf("Error recording time to setup, %s", err.Error())
	}
	startProcessing = time.Now()
	defer func() {
		if err := promPush.PushPromDuration(startProcessing, "check-certs", "processing"); err != nil {
			log.WithFields(log.Fields{"caller": "main"}).Errorf("Error recording time to setup, %s", err.Error())
		}
	}()

	// Now actually ingest the service certs, check expiration dates against warning
	log.WithField("caller", "main").Info("Ingesting service certs")
	defer wg.Wait()

	for _, eConfig := range exptConfigs {
		wg.Add(1)
		warnMsg := "%s\t%d days\t%s"
		go func(e experiment.ExptConfig) {
			defer wg.Done()
			for account := range e.Accounts {
				var certFile, keyFile string

				// Get cert, key paths
				if e.CertFile != "" {
					certFile = e.CertFile
				} else {
					certFile = path.Join(e.CertBaseDir, account+".cert")
				}

				if e.KeyFile != "" {
					keyFile = e.KeyFile
				} else {
					keyFile = path.Join(e.CertBaseDir, account+".key")
				}

				// Create service cert objects
				s, err := proxy.NewServiceCert(ctx, certFile, keyFile)
				if err != nil || s == nil {
					msg := "Could not ingest service certificate from cert and key file"
					log.WithField("experiment", e.Name).Error(msg)
					nMsg := msg + " for experiment " + e.Name
					nMgr <- notifications.Notification{
						Msg:       nMsg,
						AdminOnly: true,
					}
					return
				}

				certExpiration[s.DN] = s.Expiration
				timeLeft := time.Until(s.Expiration)

				if timeLeft < tConfig["expirewarningcutoffduration"] {
					numDays := math.Round(timeLeft.Hours() / 24.0)
					log.WithFields(log.Fields{
						"experiment": e.Name,
						"DN":         s.DN,
						"daysLeft":   numDays,
					}).Warn("Service cert expiring soon")
					nMgr <- notifications.Notification{
						Msg:       fmt.Sprintf(warnMsg, account, numDays, s.DN),
						AdminOnly: true,
					}
					existsExpiringCerts = true
				}

			}
		}(eConfig)

	}

	wg.Wait()
}
