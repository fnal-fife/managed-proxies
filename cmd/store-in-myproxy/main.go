package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/user"
	"path"
	"strconv"
	"sync"
	"time"

	//"github.com/jinzhu/copier"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
	"github.com/rifflock/lfshook"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	"cdcvs.fnal.gov/discompsupp/ken_proxy_push/experiment"
	"cdcvs.fnal.gov/discompsupp/ken_proxy_push/jobsubServerUtils"
	"cdcvs.fnal.gov/discompsupp/ken_proxy_push/notifications"
	"cdcvs.fnal.gov/discompsupp/ken_proxy_push/proxy"
)

const configFile string = "proxy_push.yml"

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
	startSetup = time.Now()

	pflag.StringP("experiment", "e", "", "Name of single experiment whose proxies should be stored in MyProxy")
	pflag.StringP("configfile", "c", configFile, "Specify alternate config file")
	pflag.BoolP("test", "t", false, "Test mode (proxies not stored in MyProxy)")

	pflag.Parse()
	viper.BindPFlags(pflag.CommandLine)

	// Read the config file
	viper.SetConfigFile(viper.GetString("configfile"))
	viper.AddConfigPath("/etc/managed-proxies/")
	viper.AddConfigPath("$HOME/managed-proxies/")
	viper.AddConfigPath(".")
	err := viper.ReadInConfig()
	if err != nil {
		panic(fmt.Errorf("Fatal error config file: %s", err))
	}

	// Set up the logConfig to pass to other packages
	//	lConfig = make(proxyPushLogger.LogsConfig)
	//	for key, value := range viper.GetStringMapString("logs") {
	//		lConfig[key] = value
	//	}
	//
	//	log = proxyPushLogger.New("", lConfig)

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
	nConfig.IsTest = viper.GetBool("test")
	nConfig.From = viper.GetString("notifications.admin_email")
	// TODO  Add datestamp
	nConfig.Subject = "Managed Proxy Service Errors - push to MyProxy"

	setAdminEmail(&nConfig)

	// Now that our log is set up and we've got a valid config, handle all init (fatal) errors using the following func
	// that logs the error, sends a slack message and an email, cleans up, and then exits.
	initErrorNotify := func(m string) {
		log.WithFields(log.Fields{"caller": "main.init"}).Error(m)
		nConfig.Subject = "Error setting up myProxy store"

		// Durations are hard-coded here since we haven't parsed them out yet
		slackInitCtx, slackInitCancel := context.WithTimeout(context.Background(), time.Duration(15*time.Second))
		notifications.SendSlackMessage(slackInitCtx, nConfig, m)
		slackInitCancel()

		//		if _, err := os.Stat(viper.GetString("logs.errfile")); !os.IsNotExist(err) {
		//			if e := os.Remove(viper.GetString("logs.errfile")); e != nil {
		//				log.Warn("Could not remove error file.  Please remove manually")
		//			}
		//}
		os.Exit(1)
	}

	// Check that we're running as the right user
	if err = checkUser(viper.GetString("global.should_runuser")); err != nil {
		initErrorNotify(err.Error())
	}

	// Parse our timeouts, store them into timeoutDurationMap for later use
	tConfig = make(map[string]time.Duration)

	for timeoutName, timeoutString := range viper.GetStringMapString("timeout") {
		value, err := time.ParseDuration(timeoutString)
		if err != nil {
			msg := fmt.Sprintf("Invalid %s value: %s", timeoutName, timeoutString)
			initErrorNotify(msg)
		}
		newName := timeoutName + "Duration"
		tConfig[newName] = value
	}

	// Kerb config
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
	ctx, cancel := context.WithTimeout(context.Background(), tConfig["globaltimeoutDuration"])
	defer cancel()

	// Start notifications manager, just for admin
	nwg.Add(1)
	defer nwg.Wait()
	nMgr := notifications.NewManager(ctx, &nwg, nConfig)
	defer close(nMgr)

	// Get list of experiments
	exptConfigs := make([]experiment.ExptConfig, 0, len(viper.GetStringMap("experiments"))) // Slice of experiment configurations
	if viper.GetString("experiment") != "" {
		// If experiment is passed in on command line
		eConfig, err := createExptConfig(viper.GetString("experiment"))
		if err != nil {
			log.WithFields(log.Fields{
				"experiment": viper.GetString("experiment"),
				"caller":     "main",
			}).Error("Error setting up experiment configuration slice.  As this is the only experiment, we will cleanup now.")
			// TODO:  Cleanup, if any
			os.Exit(1)
		}
		exptConfigs = append(exptConfigs, eConfig)
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
		}
	}

	// Get jobsub server information
	jobsubServerUtils.StartHTTPSClient(viper.GetString("global.capath"))

	retrievers, err := jobsubServerUtils.GetRetrievers(ctx, viper.GetString("global.jobsubserver"), viper.GetString("global.cigetcertoptsendpoint"))
	if err != nil {
		log.WithField("caller", "main").Error("Error getting trusted retrievers from cigetcertopts file")
		os.Exit(1)
	}

	if err := jobsubServerUtils.CheckRetrievers(retrievers, viper.GetString("global.defaultretrievers")); err != nil {
		log.WithField("caller", "main").Error(err)
	}

	// Setup is done here.  Push the time TODO:  Change this method to push to myproxy metric
	if err := promPush.PushPromDuration(startSetup, "setup"); err != nil {
		log.WithFields(log.Fields{"caller": "main"}).Errorf("Error recording time to setup, %s", err.Error())
	}
	startProcessing = time.Now()

	// Now actually ingest those service certs, generate grid proxies, and store them in myproxy LEFT OFF HERE
	log.WithField("caller", "main").Info("Ingesting service certs")
	defer wg.Wait()

	for _, eConfig := range exptConfigs {
		wg.Add(1)
		go func(e experiment.ExptConfig) {
			defer wg.Done()
			for account := range e.Accounts {
				var certFile, keyFile string

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

				s, err := proxy.NewServiceCert(ctx, certFile, keyFile)
				if err != nil {
					msg := "Could not ingest service certificate from cert and key file"
					log.WithField("experiment", e.Name).Error(msg)
					nMsg := msg + " for experiment " + e.Name
					nMgr <- notifications.Notification{
						Msg:       nMsg,
						AdminOnly: true,
					}
					return
				}

				gCtx, gCancel := context.WithTimeout(ctx, tConfig["gpitimeoutDuration"])
				defer gCancel()
				g, err := proxy.NewGridProxy(gCtx, s, tConfig["gpivalidDuration"])
				if err != nil {
					msg := "Could not generate grid proxy object"
					log.WithField("experiment", e.Name).Error(msg)
					nMsg := msg + " for experiment " + e.Name
					nMgr <- notifications.Notification{
						Msg:       nMsg,
						AdminOnly: true,
					}
					return
				}
				defer g.Remove()

				if viper.GetBool("test") {
					log.WithField("experiment", e.Name).Info("Test mode.  Stopping here")
					return
				}

				mCtx, mCancel := context.WithTimeout(ctx, tConfig["myproxystoretimeoutDuration"])
				defer mCancel()
				if err := proxy.StoreInMyProxy(mCtx, g, retrievers, viper.GetString("global.myproxyserver"), tConfig["gpivalidDuration"]); err != nil {
					msg := "Could not store grid proxy in myproxy"
					log.WithField("experiment", e.Name).Error(msg)
					nMsg := msg + " for experiment " + e.Name
					nMgr <- notifications.Notification{
						Msg:       nMsg,
						AdminOnly: true,
					}
				} else {
					log.WithFields(log.Fields{
						"experiment": e.Name,
						"dn":         g.DN,
					}).Info("Stored grid proxy in myproxy")
				}
			}
		}(eConfig)

	}

	wg.Wait()
	// Push prometheus time

}

// setAdminEmail sets the notifications config objects' From and To fields to the config file's admin value
func setAdminEmail(pnConfig *notifications.Config) {
	pnConfig.From = pnConfig.ConfigInfo["admin_email"]
	pnConfig.To = []string{pnConfig.ConfigInfo["admin_email"]}
	log.Debug("Set notifications config email values to admin defaults")
	return
}

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

// createExptConfig takes the config information from the global file and creates an exptConfig object
// TODO  This has to handle default case - certbasedir/account.cert.  Also need to call this multiple times per experiment
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
		vomsprefix = viper.GetString("vomsproxyinit.defaultvomsprefixroot") + expt + "/"
	}

	if exptSubConfig.IsSet("certfile") {
		certfile = exptSubConfig.GetString("certfile")
	}
	if exptSubConfig.IsSet("keyfile") {
		keyfile = exptSubConfig.GetString("keyfile")
	}

	// Notifications setup
	//	n := notifications.Config{}
	//	copier.Copy(&n, &nConfig)
	//	n.Experiment = expt
	//	n.From = viper.GetString("notifications.admin_email")
	//	if !viper.GetBool("test") {
	//		n.To = exptSubConfig.GetStringSlice("emails")
	//	}
	//	n.Subject = "Managed Proxy Push errors for " + expt

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
	}

	// Put this on to set the notifications logger

	log.Debug("Set up experiment config")
	return c, nil

}
