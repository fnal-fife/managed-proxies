package main

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"os"
	"path"
	"sort"
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
	"cdcvs.fnal.gov/discompsupp/ken_proxy_push/packaging"
	"cdcvs.fnal.gov/discompsupp/ken_proxy_push/proxy"
)

const configFile string = "managedProxies"

var (
	nConfig notifications.Config
	tConfig map[string]time.Duration

	startSetup      time.Time
	startProcessing time.Time
	prometheusUp    bool
	promPush        notifications.BasicPromPush
)

func init() {
	var nKey string
	startSetup = time.Now()

	viper.SetDefault("notifications.admin_email", "fife-group@fnal.gov")

	pflag.StringP("configfile", "c", "", "Specify alternate config file")
	pflag.BoolP("test", "t", false, "Test mode (no email sent)")
	pflag.Bool("version", false, "Version of Managed Proxies library")

	pflag.Parse()
	viper.BindPFlags(pflag.CommandLine)

	if viper.GetBool("version") {
		fmt.Printf("Managed Proxies version %s\n", packaging.Version)
		os.Exit(0)
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

	// Set up logs
	log.SetLevel(log.DebugLevel)
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
	var wg sync.WaitGroup
	mux := &sync.Mutex{}
	var needAlarm bool
	var templateFile = viper.GetString("checkcerts.templateOK")
	cNotes := make([]notifications.CertExpirationNotification, 0)
	ctx, cancel := context.WithTimeout(context.Background(), tConfig["globaltimeoutDuration"])
	defer cancel()

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
	// After we exit, push processing timestamp
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
					return
				}

				// Figure out how much time is left
				timeLeft := time.Until(s.Expiration)
				numDays := int(math.Round(timeLeft.Hours() / 24.0))
				c := notifications.CertExpirationNotification{
					Account:  account,
					DN:       s.DN,
					DaysLeft: numDays,
				}

				if timeLeft < tConfig["expirewarningcutoffDuration"] {
					log.WithFields(log.Fields{
						"experiment": e.Name,
						"DN":         s.DN,
						"daysLeft":   numDays,
					}).Warn("Service cert expiring soon")
					c.Warn = true
					needAlarm = true
				}
				mux.Lock()
				cNotes = append(cNotes, c)
				mux.Unlock()
			}
		}(eConfig)

	}

	wg.Wait()

	// Send notification
	fNotes := make([]notifications.CertExpirationNotification, 0)

	if needAlarm {
		templateFile = viper.GetString("checkcerts.templateAlarm")
		for _, c := range cNotes {
			if c.Warn {
				fNotes = append(fNotes, c)
			}
		}
		nConfig.Subject = nConfig.Subject + " - ALARMS"
	} else {
		sort.SliceStable(cNotes, func(i, j int) bool {
			return cNotes[i].DaysLeft < cNotes[j].DaysLeft
		})
		fNotes = append(fNotes, cNotes[0])
		nConfig.Subject = nConfig.Subject + " - no issues"
	}

	if err := notifications.SendCertAlarms(ctx, nConfig, fNotes, templateFile); err != nil {
		log.WithField("caller", "main").Error("Error sending Cert Alarms")
	}

}
