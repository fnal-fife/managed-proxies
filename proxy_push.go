package main

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/user"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/rifflock/lfshook"
	"github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	//	gomail "gopkg.in/gomail.v2"
)

// test mode - IN PROGRESS
//notifications	// gomail https://godoc.org/gopkg.in/gomail.v2#Message.SetBody  go-slack?  net/http, notifications change!
// Error handling - break everything!

const (
	globalTimeout uint   = 30                           // Global timeout in seconds
	exptTimeout   uint   = 20                           // Experiment timeout in seconds
	configFile    string = "proxy_push_config_test.yml" // CHANGE ME BEFORE PRODUCTION
)

// Global logger
var log = logrus.New()

type experimentSuccess struct {
	name    string
	success bool
}

type pingNodeStatus struct {
	node string
	err  error
}

type vomsProxyStatus struct {
	filename string
	err      error
}

type copyProxiesStatus struct {
	node    string
	account string
	role    string
	err     error
}

// Experiment worker-specific functions

func exptLogInit(ename string, logconfig map[string]string) *logrus.Entry {
	var Log = logrus.New()
	exptlogfilename := "golang_proxy_push_" + ename + ".log" // Remove GOLANG before production

	// remove the golang stuff for production
	logfilename := fmt.Sprintf("golang%s", logconfig["logfile"])
	errfilename := fmt.Sprintf("golang%s", logconfig["errfile"])

	Log.SetLevel(logrus.DebugLevel)

	// General Log
	Log.AddHook(lfshook.NewHook(lfshook.PathMap{
		logrus.DebugLevel: logfilename,
		logrus.InfoLevel:  logfilename,
		logrus.WarnLevel:  logfilename,
		logrus.ErrorLevel: logfilename,
		logrus.FatalLevel: logfilename,
		logrus.PanicLevel: logfilename,
	}))

	// Error log
	Log.AddHook(lfshook.NewHook(lfshook.PathMap{
		logrus.ErrorLevel: errfilename,
		logrus.FatalLevel: errfilename,
		logrus.PanicLevel: errfilename,
	}))

	// Experiment-specific log
	Log.AddHook(lfshook.NewHook(lfshook.PathMap{ // For production, take out all until ErrorLevel
		logrus.DebugLevel: exptlogfilename,
		logrus.InfoLevel:  exptlogfilename,
		logrus.WarnLevel:  exptlogfilename,
		logrus.ErrorLevel: exptlogfilename,
		logrus.FatalLevel: exptlogfilename,
		logrus.PanicLevel: exptlogfilename,
	}))

	exptlog := Log.WithFields(logrus.Fields{"experiment": ename})

	exptlog.Info("Set up experiment logger")

	return exptlog
}

func getKerbTicket(krb5ccname string) error {
	os.Setenv("KRB5CCNAME", krb5ccname)

	kerbcmdargs := []string{"-k", "-t",
		"/opt/gen_keytabs/config/gcso_monitor.keytab",
		"monitor/gcso/fermigrid.fnal.gov@FNAL.GOV"}

	cmd := exec.Command("/usr/krb5/bin/kinit", kerbcmdargs...)
	cmdOut, cmdErr := cmd.CombinedOutput()
	if cmdErr != nil {
		return fmt.Errorf("Initializing a kerb ticket failed.  The error was %s: %s", cmdErr, cmdOut)
	}
	return nil
}

func checkKeys(exptConfig *viper.Viper) error {
	// Nodes and Roles
	if !exptConfig.IsSet("nodes") || !exptConfig.IsSet("accounts") {
		return errors.New(`Input file improperly formatted for %s (accounts or nodes don't 
			exist for this experiment). Please check the config file on fifeutilgpvm01.
			 I will skip this experiment for now.`)
	}
	return nil
}

func pingAllNodes(nodes []string) <-chan pingNodeStatus {
	c := make(chan pingNodeStatus, len(nodes))
	for _, node := range nodes {
		go func(node string) {
			p := pingNodeStatus{node, nil}
			pingargs := []string{"-W", "5", "-c", "1", node}
			cmd := exec.Command("ping", pingargs...)
			cmdOut, cmdErr := cmd.CombinedOutput()
			if cmdErr != nil {
				p.err = fmt.Errorf("%s %s", cmdErr, cmdOut)
			}
			c <- p
		}(node)
	}
	return c
}

func getProxies(exptConfig *viper.Viper, globalConfig map[string]string, exptname string) <-chan vomsProxyStatus {
	c := make(chan vomsProxyStatus)
	var vomsprefix, certfile, keyfile string

	if exptConfig.IsSet("vomsgroup") {
		vomsprefix = exptConfig.GetString("vomsgroup")
	} else {
		vomsprefix = "fermilab:/fermilab/" + exptname + "/"
	}

	for account, role := range exptConfig.GetStringMapString("accounts") {
		go func(account, role string) {

			vomsstring := vomsprefix + "Role=" + role

			if exptConfig.IsSet("certfile") {
				certfile = exptConfig.GetString("certfile")
			} else {
				certfile = path.Join(globalConfig["cert_base_dir"], account+".cert")
			}

			if exptConfig.IsSet("keyfile") {
				keyfile = exptConfig.GetString("keyfile")
			} else {
				keyfile = path.Join(globalConfig["cert_base_dir"], account+".key")
			}

			outfile := account + "." + role + ".proxy"
			outfilePath := path.Join("proxies", outfile)

			vpi := vomsProxyStatus{outfile, nil}
			vpiargs := []string{"-rfc", "-valid", "24:00", "-voms",
				vomsstring, "-cert", certfile,
				"-key", keyfile, "-out", outfilePath}

			cmd := exec.Command("/usr/bin/voms-proxy-init", vpiargs...)
			cmdErr := cmd.Run()
			if cmdErr != nil {
				err := fmt.Sprintf(`Error obtaining %s.  Please check the cert on 
				  fifeutilgpvm01. \n%s Continuing on to next role.`, outfile, cmdErr)
				vpi.err = errors.New(err)
			}
			// if e == "darkside" {
			// 	time.Sleep(time.Duration(10) * time.Second)
			// }

			c <- vpi
		}(account, role)
	}
	return c
}

func copyProxies(exptConfig *viper.Viper) <-chan copyProxiesStatus {
	c := make(chan copyProxiesStatus)
	// One copy per node and role
	for acct, role := range exptConfig.GetStringMapString("accounts") {
		go func(acct, role string) {
			proxyFile := acct + "." + role + ".proxy"
			proxyFilePath := path.Join("proxies", proxyFile)

			for _, node := range exptConfig.GetStringSlice("nodes") {
				go func(acct, role, node string) {
					cps := copyProxiesStatus{node, acct, role, nil}
					accountNode := acct + "@" + node + ".fnal.gov"
					newProxyPath := path.Join(exptConfig.GetString("dir"), acct, proxyFile+".new")
					finalProxyPath := path.Join(exptConfig.GetString("dir"), acct, proxyFile)

					sshopts := []string{"-o", "ConnectTimeout=30",
						"-o", "ServerAliveInterval=30",
						"-o", "ServerAliveCountMax=1"}

					scpargs := append(sshopts, proxyFilePath, accountNode+":"+newProxyPath)
					sshargs := append(sshopts, accountNode, "chmod 400 "+newProxyPath+" ; mv -f "+newProxyPath+" "+finalProxyPath)
					scpCmd := exec.Command("scp", scpargs...)
					sshCmd := exec.Command("ssh", sshargs...)

					cmdOut, cmdErr := scpCmd.CombinedOutput()
					if cmdErr != nil {
						msg := fmt.Errorf("Copying proxy %s to node %s failed.  The error was %s: %s", proxyFile, node, cmdErr, cmdOut)
						cps.err = msg
						c <- cps
						return
					}

					cmdOut, cmdErr = sshCmd.CombinedOutput()
					if cmdErr != nil {
						msg := fmt.Errorf("Error changing permission of proxy %s to mode 400 on %s.  The error was %s: %s", proxyFile, node, cmdErr, cmdOut)
						cps.err = msg
						c <- cps
						return
					}
					c <- cps
				}(acct, role, node)
			}
		}(acct, role)
	}
	return c
}

func sendExperimentEmail(exptConfig *viper.Viper, logfilepath string) error {
	// func to send email of logfile
	return nil
}

func (expt *experimentSuccess) experimentCleanup(exptConfig *viper.Viper) error {
	exptlogfilename := "golang_proxy_push_" + expt.name + ".log" // Remove GOLANG before production

	dir, e := os.Getwd()
	if e != nil {
		return errors.New(`Could not get current working directory.  Aborting cleanup.  
					Please check working directory and manually clean up log files`)
	}

	exptlogfilepath := path.Join(dir, exptlogfilename)

	// No experiment logfile
	if _, err := os.Stat(exptlogfilepath); os.IsNotExist(err) {
		return nil
	}

	// Successful experiment, but no errors in log file.  Probably the default option
	if expt.success {
		if err := os.Remove(exptlogfilepath); err != nil {
			return fmt.Errorf("Could not remove successful experiment log %s.  Please clean up manually", exptlogfilepath)
		}
	}

	if !expt.success {
		// Try to send email, which also deletes expt file, returns error
		// var err error = nil // Dummy
		// err := exptConfig.sendExperimentEmail(exptlogfilepath)
		err := errors.New("Dummy error for email") // Take this line out and replace it with
		if err != nil {
			archiveLogDir := path.Join(dir, "experiment_log_archive")
			if _, e = os.Stat(archiveLogDir); os.IsNotExist(e) {
				archiveLogDir = dir
			}

			// oldpath := exptlogfile
			newfilename := fmt.Sprintf("%s-%s", exptlogfilename, time.Now().Format(time.RFC3339))
			newpath := path.Join(archiveLogDir, newfilename)

			if e = os.Rename(exptlogfilepath, newpath); e != nil {
				return fmt.Errorf("Could not move file %s to %s.  The error was %v", exptlogfilepath, newpath, e)
			}
			return fmt.Errorf("Could not send email for experiment %s.  Archived error file at %s", expt.name, newpath)
		}
	}

	return nil
}

func experimentWorker(exptname string) <-chan experimentSuccess {
	c := make(chan experimentSuccess)
	expt := experimentSuccess{exptname, true} // Initialize
	exptLog := exptLogInit(expt.name, viper.GetStringMapString("logs"))

	exptLog.Info("Now processing ", expt.name)
	go func() {
		exptConfig := viper.Sub("experiments." + expt.name)

		badnodes := make(map[string]struct{})
		successfulCopies := make(map[string][]string)

		for _, node := range exptConfig.GetStringSlice("nodes") {
			badnodes[node] = struct{}{}
		}

		// if e == "darkside" {
		// 	time.Sleep(20 * time.Second)
		// }

		if !viper.IsSet("global.krb5ccname") {
			exptLog.Error(`Could not obtain KRB5CCNAME environmental variable from
				config.  Please check the config file on fifeutilgpvm01.`)
			expt.success = false
			c <- expt
			close(c)
			return
		}
		krb5ccnameCfg := viper.GetString("global.krb5ccname")

		// If we can't get a kerb ticket, log error and keep going.
		// We might have an old one that's still valid.
		if err := getKerbTicket(krb5ccnameCfg); err != nil {
			exptLog.Error(err)
		}

		// If check of exptConfig keys fails, experiment fails immediately
		if err := checkKeys(exptConfig); err != nil {
			exptLog.Error(err)
			expt.success = false
			c <- expt
			close(c)
			return
		}

		pingChannel := pingAllNodes(exptConfig.GetStringSlice("nodes"))
		for _ = range exptConfig.GetStringSlice("nodes") { // Note that we're iterating over the range of nodes so we make sure
			// that we listen on the channel the right number of times
			select {
			case testnode := <-pingChannel:
				if testnode.err == nil {
					delete(badnodes, testnode.node)
				} else {
					exptLog.Error(testnode.err)
				}
			case <-time.After(time.Duration(10) * time.Second):
			}
		}

		badNodesSlice := make([]string, 0, len(badnodes))
		for node := range badnodes {
			badNodesSlice = append(badNodesSlice, node)
		}

		if len(badNodesSlice) > 0 {
			exptLog.Warn("Bad nodes are: ", badNodesSlice)
		}

		// If voms-proxy-init fails, we'll just continue on.  We'll still try to push proxies,
		// since they're valid for 24 hours
		vpiChan := getProxies(exptConfig, viper.GetStringMapString("global"), expt.name)
		for _ = range exptConfig.GetStringMapString("accounts") {
			select {
			case vpi := <-vpiChan:
				if vpi.err != nil {
					exptLog.Error(vpi.err)
					expt.success = false
				} else {
					exptLog.Debug("Generated voms proxy: ", vpi.filename)
				}
			case <-time.After(time.Duration(5) * time.Second):
				exptLog.Errorf("Error obtaining proxy for %s:  timeout.  Check log for details. Continuing to next proxy.\n", expt.name)
				expt.success = false
			}
		}

		copyChan := copyProxies(exptConfig)
		exptTimeoutChan := time.After(time.Duration(exptTimeout) * time.Second)
		for _ = range exptConfig.GetStringSlice("nodes") {
			for _ = range exptConfig.GetStringMapString("accounts") {
				select {
				case pushproxy := <-copyChan:
					if pushproxy.err != nil {
						exptLog.Error(pushproxy.err)
						expt.success = false
					} else {
						successfulCopies[pushproxy.role] = append(successfulCopies[pushproxy.role], pushproxy.node)
					}
				case <-exptTimeoutChan:
					exptLog.Error("Experiment hit the timeout when waiting to push proxy.")
					expt.success = false
				}
			}
		}

		for role, nodes := range successfulCopies {
			sort.Strings(nodes)
			exptLog.Debugf("Successful copies for role %s were %v", role, nodes)
		}
		exptLog.Info("Finished processing ", expt.name)
		c <- expt
		close(c)

		// We're logging the cleanup in the general log so that we don't create an extraneous
		// experiment log file
		if err := expt.experimentCleanup(exptConfig); err != nil {
			log.Error(err)
		}
		log.Info("Finished cleaning up ", expt.name)
	}()
	return c
}

// Global functions

func parseFlags() {
	pflag.StringP("experiment", "e", "", "Name of single experiment to push proxies")
	pflag.StringP("configfile", "c", configFile, "Specify alternate config file")
	pflag.BoolP("test", "t", false, "Test mode")

	pflag.Parse()

	viper.BindPFlags(pflag.CommandLine)
	return
}

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

func manageExperimentChannels(exptList []string) <-chan experimentSuccess {
	agg := make(chan experimentSuccess)
	exptChans := make([]<-chan experimentSuccess, 0, len(exptList))
	var i int // Counter to keep track of how many times we've sent over agg channel

	go func() {
		// Start all of the experiment workers
		for _, expt := range exptList {
			exptChans = append(exptChans, experimentWorker(expt))
		}

		// Launch goroutines that listen on experiment channels.  Since each experimentWorker closes its channel,
		// each of these goroutines should exit after that happens
		for _, exptChan := range exptChans {
			go func(c <-chan experimentSuccess) {
				for expt := range c {
					agg <- expt
					i++
				}
			}(exptChan)
		}

		// Close out agg channel when we're done sending
		for {
			if i == len(exptList) {
				log.Debug("Closing aggregation channel")
				close(agg)
				return
			}
		}

	}()
	return agg
}

func loginit(logconfig map[string]string) {
	// remove the golang stuff for production
	logfilename := fmt.Sprintf("golang%s", logconfig["logfile"])
	errfilename := fmt.Sprintf("golang%s", logconfig["errfile"])

	// Set up our global logger
	log.Level = logrus.DebugLevel

	logFormatter := logrus.TextFormatter{FullTimestamp: true}

	log.Formatter = &logFormatter

	// Error log
	log.AddHook(lfshook.NewHook(lfshook.PathMap{
		logrus.ErrorLevel: errfilename,
		logrus.FatalLevel: errfilename,
		logrus.PanicLevel: errfilename,
	}))

	// General Log
	log.AddHook(lfshook.NewHook(lfshook.PathMap{
		logrus.DebugLevel: logfilename,
		logrus.InfoLevel:  logfilename,
		logrus.WarnLevel:  logfilename,
		logrus.ErrorLevel: logfilename,
		logrus.FatalLevel: logfilename,
		logrus.PanicLevel: logfilename,
	}))

}

func cleanup(exptStatus map[string]bool, experiments []string) {
	s := make([]string, 0, len(experiments))
	f := make([]string, 0, len(experiments))

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

	// Something in here to delete/archive temp log dir if needed

	// tempFiles, err := ioutil.ReadDir()

	return
}

func main() {
	exptSuccesses := make(map[string]bool) // map of successful expts

	parseFlags()

	// Read the config file
	viper.SetConfigFile(viper.GetString("configfile"))
	err := viper.ReadInConfig()
	if err != nil {
		panic(fmt.Errorf("Fatal error config file: %s", err))
	}

	// Set up our logger
	// From here on out, we're logging to the log file too
	loginit(viper.GetStringMapString("logs"))
	log.Debugf("Using config file %s", viper.GetString("configfile"))

	// Test flag sets which notifications section from config we want to use.
	// After this, cfg.Notifications map is the map we want to use later on. -- check this
	if viper.GetBool("test") {
		log.Info("Running in test mode")
		viper.Set("notifications", viper.Get("notifications_test"))
		// cfg.Notifications = cfg.Notifications_test
	}

	// Check that we're running as the right user
	if err = checkUser(viper.GetString("global.should_runuser")); err != nil {
		log.Error(err)
		os.Exit(3)
	}

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
	c := manageExperimentChannels(expts)
	// Listen on the manager channel
	timeout := time.After(time.Duration(globalTimeout) * time.Second)
	for {
		select {
		case expt, chanOK := <-c:
			if !chanOK {
				cleanup(exptSuccesses, expts)
				return
			}
			exptSuccesses[expt.name] = expt.success
		case <-timeout:
			log.Error("Hit the global timeout!")
			cleanup(exptSuccesses, expts)
			return
		}
	}
}
