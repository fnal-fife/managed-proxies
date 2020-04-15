package main

import (
	"context"
	"errors"
	"fmt"
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

	"cdcvs.fnal.gov/discompsupp/ken_proxy_push/v4/notifications"
	"cdcvs.fnal.gov/discompsupp/ken_proxy_push/v4/packaging"
	"cdcvs.fnal.gov/discompsupp/ken_proxy_push/v4/proxy"
	"cdcvs.fnal.gov/discompsupp/ken_proxy_push/v4/utils"
	"cdcvs.fnal.gov/discompsupp/ken_proxy_push/v4/utils/storeinmyproxy"
)

const configFile string = "managedProxies"

var (
	nConfig notifications.Config
	tConfig utils.TimeoutsConfig

	startSetup      time.Time
	startProcessing time.Time
	prometheusUp    bool
	promPush        notifications.BasicPromPush
	buildTimestamp  string
)

func init() {
	var nKey string
	startSetup = time.Now()

	viper.SetDefault("notifications.admin_email", "fife-group@fnal.gov")

	pflag.StringP("experiment", "e", "", "Name of single experiment whose proxies should be stored in MyProxy")
	pflag.StringP("configfile", "c", "", "Specify alternate config file")
	pflag.BoolP("test", "t", false, "Test mode (proxies not stored in MyProxy)")
	pflag.Bool("version", false, "Version of Managed Proxies library")
	pflag.String("admin", "", "Override the config file admin email")

	pflag.Parse()
	viper.BindPFlags(pflag.CommandLine)

	if viper.GetBool("version") {
		fmt.Printf("Managed Proxies Version %s, Build %s\n", packaging.Version, buildTimestamp)
		os.Exit(0)
	}

	if viper.GetString("admin") != "" {
		if !utils.EmailRegexp.MatchString(viper.GetString("admin")) {
			fmt.Printf("Admin email address %s is invalid!  It must follow the regexp %s\n", viper.GetString("admin"), utils.EmailRegexp.String())
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
	nConfig.Experiment = "all experiments"
	nConfig.IsTest = viper.GetBool("test")
	nConfig.From = viper.GetString("notifications.admin_email")
	timestamp := time.Now().Format(time.RFC822)
	nConfig.Subject = fmt.Sprintf("Managed Proxy Service Errors - push to MyProxy - %s", timestamp)

	// Set From and To to admin email
	nConfig.From = nConfig.ConfigInfo["admin_email"]
	nConfig.To = []string{nConfig.ConfigInfo["admin_email"]}
	if flagAdminEmail := viper.GetString("admin"); flagAdminEmail != "" {
		nConfig.To = []string{flagAdminEmail}
	}
	log.Debug("Set notifications config email values to admin values")

	// Now that our log is set up and we've got a valid config, handle all init (fatal) errors using the following func
	// that logs the error, sends a slack message and an email, cleans up, and then exits.
	initErrorNotify := func(m string) {
		log.WithFields(log.Fields{"caller": "main.init"}).Error(m)
		nConfig.Subject = "Error setting up store-in-myproxy"

		// Durations are hard-coded here since we haven't parsed them out yet
		slackInitCtx, slackInitCancel := context.WithTimeout(context.Background(), time.Duration(15*time.Second))
		defer slackInitCancel()
		s := notifications.SlackMessage{}
		s.SendMessage(slackInitCtx, m, nConfig.ConfigInfo)
		os.Exit(1)
	}

	// Check that we're running as the right user
	if err := utils.CheckUser(viper.GetString("global.authuser")); err != nil {
		initErrorNotify(err.Error())
	}

	// Parse our timeouts, store them into timeoutDurationMap for later use
	tConfig = make(utils.TimeoutsConfig)

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
	var nwg, wg sync.WaitGroup
	var exptFailures map[string]map[string]error
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
		if err := notifications.SendAdminNotifications(ctx, nConfig, "store-in-myproxy"); err != nil {
			log.WithField("caller", "main").Error("Error sending Admin Notifications")
		}
	}()

	nwg.Add(1)
	defer nwg.Wait()
	nMgr := notifications.NewManager(ctx, &nwg, nConfig)
	defer close(nMgr)

	// Get list of experiments
	exptConfigs := make([]*utils.ExptConfig, 0, len(viper.GetStringMap("experiments"))) // Slice of experiment configurations
	expts := make([]string, 0, len(exptConfigs))

	getExptKey := func(expt string) string {
		exptKey := "experiments." + expt
		if !viper.IsSet(exptKey) {
			err := errors.New("Experiment is not configured in the configuration file")
			log.WithFields(log.Fields{
				"experiment": expt,
			}).Panic(err)
		}
		return exptKey
	}

	// Functional options to send to CreateExptConfig
	setGlobalCertBaseDir := func(e *utils.ExptConfig) {
		e.CertBaseDir = viper.GetString("global.cert_base_dir")
	}

	setExptConfigAccounts := func(e *utils.ExptConfig) {
		key := getExptKey(e.Name) + ".accounts"
		e.Accounts = viper.GetStringMapString(key)
	}

	setExptCertandKeyFile := func(e *utils.ExptConfig) {
		exptSubConfig := viper.Sub(getExptKey(e.Name))
		if exptSubConfig.IsSet("certfile") && exptSubConfig.IsSet("keyfile") {
			e.CertFile = exptSubConfig.GetString("certfile")
			e.KeyFile = exptSubConfig.GetString("keyfile")
		}
	}

	withTimeoutsConfig := func(e *utils.ExptConfig) {
		e.TimeoutsConfig = tConfig
	}

	setTestModebyFlag := func(e *utils.ExptConfig) {
		if viper.GetBool("test") {
			e.IsTest = true
		}
	}

	// Get our list of experiments from the config file, create exptConfig objects
	if viper.GetString("experiment") != "" {
		// If experiment is passed in on command line
		eConfig, err := utils.CreateExptConfig(
			viper.GetString("experiment"),
			setGlobalCertBaseDir,
			setExptConfigAccounts,
			setExptCertandKeyFile,
			withTimeoutsConfig,
			setTestModebyFlag,
		)
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
			eConfig, err := utils.CreateExptConfig(
				k,
				setGlobalCertBaseDir,
				setExptConfigAccounts,
				setExptCertandKeyFile,
				withTimeoutsConfig,
				setTestModebyFlag,
			)
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

	// Get jobsub server information
	storeinmyproxy.StartHTTPSClient(viper.GetString("global.capath"))

	// Get and check our retrievers list
	retrievers, err := storeinmyproxy.GetRetrievers(ctx, viper.GetString("global.jobsubserver"), viper.GetString("global.cigetcertoptsendpoint"))
	if err != nil {
		log.WithField("caller", "main").Error("Error getting trusted retrievers from cigetcertopts file")
		os.Exit(1)
	}

	if err := storeinmyproxy.CheckRetrievers(retrievers, viper.GetString("global.defaultretrievers")); err != nil {
		log.WithField("caller", "main").Error(err)
	}

	// Setup is done here.  Push the time
	if err := promPush.PushPromDuration(startSetup, "store-in-myproxy", "setup"); err != nil {
		log.WithFields(log.Fields{"caller": "main"}).Errorf("Error recording time to setup, %s", err.Error())
	}
	startProcessing = time.Now()
	defer func() {
		if err := promPush.PushPromDuration(startProcessing, "store-in-myproxy", "processing"); err != nil {
			log.WithFields(log.Fields{"caller": "main"}).Errorf("Error recording time to setup, %s", err.Error())
		}
	}()

	// Now actually ingest the service certs, generate grid proxies, and store them in myproxy
	log.WithField("caller", "main").Info("Ingesting service certs")
	defer wg.Wait()

	for _, eConfig := range exptConfigs {
		wg.Add(1)
		go func(e *utils.ExptConfig) {
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
					log.WithFields(log.Fields{
						"experiment": e.Name,
						"account":    account,
					}).Error(msg)
					nMsg := msg + " for experiment " + e.Name + " and account " + account
					nMgr <- notifications.Notification{
						Message:          nMsg,
						NotificationType: notifications.SetupError,
					}
					return
				}

				// Create grid proxies from those service certs
				gCtx, gCancel := context.WithTimeout(ctx, tConfig["gpitimeoutDuration"])
				defer gCancel()
				g, teardown, err := proxy.NewGridProxy(gCtx, s, tConfig["gpivalidDuration"])
				if err != nil {
					msg := "Could not generate grid proxy object"
					log.WithFields(log.Fields{
						"experiment": e.Name,
						"account":    account,
					}).Error(msg)
					nMsg := msg + " for experiment " + e.Name + " and account " + account
					nMgr <- notifications.Notification{
						Message:          nMsg,
						NotificationType: notifications.SetupError,
					}
					return
				}

				defer func() {
					r := recover()
					if r != nil {
						msg := fmt.Sprintf("Recovered from panic:  %s.  Will delete grid proxy", r)
						log.WithFields(log.Fields{
							"experiment": e.Name,
							"account":    account,
						}).Error(r)
						nMsg := msg + " for experiment " + e.Name + " and account " + account
						nMgr <- notifications.Notification{
							Message:          nMsg,
							NotificationType: notifications.SetupError,
						}
					}
					if err := teardown(); err != nil {
						nMsg := "Error deleting grid proxies.  Please check machine"
						log.Errorf(nMsg)
						nMgr <- notifications.Notification{
							Message:          nMsg,
							NotificationType: notifications.SetupError,
						}
					}

				}()

				if viper.GetBool("test") {
					log.WithFields(log.Fields{
						"experiment": e.Name,
						"account":    account,
					}).Info("Test mode.  Stopping here")
					return
				}

				// Store those grid proxies in myproxy
				mCtx, mCancel := context.WithTimeout(ctx, tConfig["myproxystoretimeoutDuration"])
				defer mCancel()
				if err := proxy.StoreInMyProxy(mCtx, g, retrievers, viper.GetString("global.myproxyserver"), tConfig["gpivalidDuration"]); err != nil {
					msg := "Could not store grid proxy in myproxy"
					log.WithField("experiment", e.Name).Error(msg)
					exptFailures[e.Name][account] = err
				} else {
					log.WithFields(log.Fields{
						"experiment":    e.Name,
						"myproxyserver": viper.GetString("global.myproxyserver"),
						"dn":            g.DN,
					}).Info("Stored grid proxy in myproxy")
					if err := promPush.PushMyProxyStoreTime(g.DN); err != nil {
						msg := "Could not push prometheus metric"
						log.WithFields(log.Fields{
							"gridProxy": g.DN,
							"metric":    "myProxyStoreTime",
						}).Error(msg)
						nMsg := msg + "myProxyStoreTime for dn " + g.DN
						nMgr <- notifications.Notification{
							Message:          nMsg,
							NotificationType: notifications.SetupError,
						}
					}
				}
			}
		}(eConfig)

	}
	wg.Wait()

	if len(exptFailures) > 0 {
		header := []string{"Experiment", "Account", "Error"}
		table := utils.DoubleErrorMapToTable(exptFailures, header)
		nMgr <- notifications.Notification{
			Message:          table,
			NotificationType: notifications.RunError,
		}
	}
}
