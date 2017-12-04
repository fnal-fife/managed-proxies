package main

import (
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"os/user"
	"path"
	"strings"
	"time"

	// "github.com/rifflock/lfshook"
	"github.com/rifflock/lfshook"
	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
	//	gomail "gopkg.in/gomail.v2"
)

// config viper?
// test mode - IN PROGRESS
// Logging - IN PROGRESS
// Optional debug for verbose mode
//notifications	// gomail https://godoc.org/gopkg.in/gomail.v2#Message.SetBody  go-slack?  net/http, notifications change!
// Error handling - break everything!

const (
	globalTimeout uint   = 30                           // Global timeout in seconds
	exptTimeout   uint   = 20                           // Experiment timeout in seconds
	configFile    string = "proxy_push_config_test.yml" // CHANGE ME BEFORE PRODUCTION
)

// Global logger
var log = logrus.New()

// var tempLogDir string

type flagHolder struct {
	experiment string
	config     string
	test       bool
}

type experimentSuccess struct {
	name    string
	success bool
}

type config struct {
	Logs               map[string]string
	Notifications      map[string]string
	Notifications_test map[string]string
	Global             map[string]string
	Experiments        map[string]*ConfigExperiment
}

type ConfigExperiment struct {
	Name      string
	Dir       string
	Emails    []string
	Nodes     []string
	Roles     map[string]string
	Vomsgroup string
	Certfile  string
	Keyfile   string
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

func parseFlags() flagHolder {
	var e = flag.String("e", "", "Name of single experiment to push proxies")
	var c = flag.String("c", configFile, "Specify alternate config file")
	var t = flag.Bool("t", false, "Test mode")

	flag.Parse()

	fh := flagHolder{*e, *c, *t}
	log.Debugf("Flags: {Experiment: %s, Alternate Config: %s, Test Mode: %v}", fh.experiment, fh.config, fh.test)
	return fh
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

func checkKeys(exptConfig *ConfigExperiment) error {
	// Nodes and Roles
	if len(exptConfig.Nodes) == 0 || len(exptConfig.Roles) == 0 {
		msg := fmt.Sprintf(`Input file improperly formatted for %s (roles or nodes don't 
			exist for this experiment). Please check the config file on fifeutilgpvm01.
			 I will skip this experiment for now.`, exptConfig.Name)
		return errors.New(msg)
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

func getProxies(exptConfig *ConfigExperiment, globalConfig map[string]string) <-chan vomsProxyStatus {
	c := make(chan vomsProxyStatus)
	var vomsprefix, certfile, keyfile string

	if exptConfig.Vomsgroup != "" {
		vomsprefix = exptConfig.Vomsgroup
	} else {
		vomsprefix = "fermilab:/fermilab/" + exptConfig.Name + "/"
	}

	for role, account := range exptConfig.Roles {
		go func(role, account string) {

			vomsstring := vomsprefix + "Role=" + role

			if exptConfig.Certfile != "" {
				certfile = exptConfig.Certfile
			} else {
				certfile = path.Join(globalConfig["CERT_BASE_DIR"], account+".cert")
			}

			if exptConfig.Keyfile != "" {
				keyfile = exptConfig.Keyfile
			} else {
				keyfile = path.Join(globalConfig["CERT_BASE_DIR"], account+".key")
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
		}(role, account)
	}
	return c
}

func copyProxies(exptConfig *ConfigExperiment) <-chan copyProxiesStatus {
	c := make(chan copyProxiesStatus)
	// One copy per node and role
	for role, acct := range exptConfig.Roles {
		go func(role, acct string) {
			proxyFile := acct + "." + role + ".proxy"
			proxyFilePath := path.Join("proxies", proxyFile)

			for _, node := range exptConfig.Nodes {
				go func(role, acct, node string) {
					cps := copyProxiesStatus{node, acct, role, nil}
					accountNode := acct + "@" + node + ".fnal.gov"
					newProxyPath := path.Join(exptConfig.Dir, acct, proxyFile+".new")
					finalProxyPath := path.Join(exptConfig.Dir, acct, proxyFile)

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
				}(role, acct, node)
			}
		}(role, acct)
	}
	return c
}

func experimentWorker(cfg config, exptConfig *ConfigExperiment) <-chan experimentSuccess {
	c := make(chan experimentSuccess)
	expt := experimentSuccess{exptConfig.Name, true}
	exptLog := exptLogInit(expt.name, cfg.Logs)

	exptLog.Info("Now processing ", expt.name)
	go func() {
		badnodes := make(map[string]struct{})

		for _, node := range exptConfig.Nodes {
			badnodes[node] = struct{}{}
		}

		// if e == "darkside" {
		// 	time.Sleep(20 * time.Second)
		// }

		if _, ok := cfg.Global["KRB5CCNAME"]; !ok {
			exptLog.Error(`Could not obtain KRB5CCNAME environmental variable from
				config.  Please check the config file on fifeutilgpvm01.`)
			expt.success = false
			c <- expt
			close(c)
			return
		}
		krb5ccnameCfg := cfg.Global["KRB5CCNAME"]

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

		pingChannel := pingAllNodes(exptConfig.Nodes)
		for _ = range exptConfig.Nodes { // Note that we're iterating over the range of nodes so we make sure
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
		vpiChan := getProxies(exptConfig, cfg.Global)
		for _ = range exptConfig.Roles {
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
		for _ = range exptConfig.Nodes {
			for _ = range exptConfig.Roles {
				select {
				case pushproxy := <-copyChan:
					if pushproxy.err != nil {
						exptLog.Error(pushproxy.err)
						expt.success = false
					}
				case <-exptTimeoutChan:
					exptLog.Error("Experiment hit the timeout when waiting to push proxy.")
					expt.success = false
				}
			}
		}
		exptLog.Info("Finished processing ", expt.name)
		c <- expt
		close(c)
	}()
	return c
}

func manageExperimentChannels(exptList []string, cfg config) <-chan experimentSuccess {
	agg := make(chan experimentSuccess)
	exptChans := make([]<-chan experimentSuccess, len(exptList))
	var i int // Counter to keep track of how many times we've sent over agg channel

	go func() {
		// Start all of the experiment workers
		for _, expt := range exptList {
			exptChans = append(exptChans, experimentWorker(cfg, cfg.Experiments[expt]))
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

	// // Check for existence of temp log dir for experiment loggers
	// if _, err := os.Stat(tempLogDir); os.IsNotExist(err) {
	// 	log.Debug("Experiment temporary log dir didn't exist (normal behavior).  Creating it now")
	// 	if err := os.Mkdir(tempLogDir, 0666); err != nil {
	// 		defer log.Warnf(`Could not create the temp log dir.
	// 			We expect experiment-specific emails to fail, but
	// 			all log messages should be in general log`, logfilename)
	// 	}
	// }

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

	// Something in here to delete temp log dir if needed

	return
}

func main() {
	var cfg config
	expts := make([]string, 0, len(cfg.Experiments)) // Slice of experiments we will actually process
	exptSuccesses := make(map[string]bool)           // map of successful expts

	// if cwd, err := os.Getwd(); err != nil {
	// 	log.Fatal("Could not get current working directory.  Exiting")
	// } else {
	// 	t := &tempLogDir
	// 	*t = path.Join(cwd, "golang_temp_log_dir") // Remove golang prefix before PRODUCTION
	// }

	// Parse flags
	flags := parseFlags()

	// Read the config file
	source, err := ioutil.ReadFile(flags.config)
	if err != nil {
		log.Error(err)
		os.Exit(2)
	}

	err = yaml.Unmarshal(source, &cfg)
	if err != nil {
		log.Error(err)
		os.Exit(2)
	}

	loginit(cfg.Logs)

	// From here on out, we're logging to the log file too
	log.Debugf("Using config file %s", flags.config)

	// Test flag sets which notifications section from config we want to use.
	// After this, cfg.Notifications map is the map we want to use later on.

	if flags.test {
		log.Info("Running in test mode")
		cfg.Notifications = cfg.Notifications_test
	}

	// Check that we're running as the right user
	if err = checkUser(cfg.Global["should_runuser"]); err != nil {
		log.Error(err)
		os.Exit(3)
	}

	// Get our list of experiments from the config file, set exptConfig Name variable
	if flags.experiment != "" {
		expts = append(expts, flags.experiment)
		ptr := cfg.Experiments[flags.experiment]
		ptr.Name = flags.experiment
	} else {
		for k := range cfg.Experiments {
			expts = append(expts, k)
			ptr := cfg.Experiments[k]
			ptr.Name = k
		}
	}

	// Start up the expt manager
	c := manageExperimentChannels(expts, cfg)
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
