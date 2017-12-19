package main

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"
	// _ "net/http/pprof"
	"os"
	"os/exec"
	"os/user"
	"path"
	// "runtime"
	"sort"
	"strings"
	"time"

	"github.com/rifflock/lfshook"
	"github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	gomail "gopkg.in/gomail.v2"
)

// Wait group for all pings, proxy copies, etc.
// chan bool done channels should be struct{} (best practice)
//notifications - IN PROGRESS - Formatting
// Error handling - break everything!

const (
	globalTimeout string = "60s" // Global timeout
	exptTimeout   string = "30s" // Experiment timeout
	slackTimeout  string = "15s" // Slack message timeout
	pingTimeout   string = "10s" // Ping timeout (total)
	vpiTimeout    string = "10s" // voms-proxy-init timeout (total)

	configFile      string = "proxy_push_config_test.yml"       // CHANGE ME BEFORE PRODUCTION
	exptLogFilename string = "golang_proxy_push_%s.log"         // CHANGE ME BEFORE PRODUCTION - temp file per experiment that will be emailed to experiment
	exptGenFilename string = "golang_proxy_push_general_%s.log" // CHANGE ME BEFORE PRODUCTION - temp file per experiment that will be copied over to logfile
)

var globalTimeoutDuration, exptTimeoutDuration, slackTimeoutDuration, pingTimeoutDuration, vpiTimeoutDuration time.Duration
var log = logrus.New()                                       // Global logger
var emailDialer = gomail.Dialer{Host: "localhost", Port: 25} // gomail dialer to use to send emails
var rwmux sync.RWMutex                                       // mutex to be used when copying experiment log into master log
var testMode = false

// Types to carry information about success and status of various operations over channels
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

func exptLogInit(ename string) *logrus.Entry {
	var Log = logrus.New()
	exptlog := fmt.Sprintf(exptLogFilename, ename)
	genlog := fmt.Sprintf(exptGenFilename, ename)

	Log.SetLevel(logrus.DebugLevel)

	// General Log that gets copied to master log
	Log.AddHook(lfshook.NewHook(lfshook.PathMap{
		logrus.DebugLevel: genlog,
		logrus.InfoLevel:  genlog,
		logrus.WarnLevel:  genlog,
		logrus.ErrorLevel: genlog,
		logrus.FatalLevel: genlog,
		logrus.PanicLevel: genlog,
	}))

	// Experiment-specific error log that gets emailed if populated
	Log.AddHook(lfshook.NewHook(lfshook.PathMap{
		logrus.ErrorLevel: exptlog,
		logrus.FatalLevel: exptlog,
		logrus.PanicLevel: exptlog,
	}))

	exptlogger := Log.WithFields(logrus.Fields{"experiment": ename})
	exptlogger.Info("Set up experiment logger")

	return exptlogger
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
			 I will skip this experiment for now`)
	}
	return nil
}

func pingAllNodes(nodes []string, wg *sync.WaitGroup, done chan struct{}) <-chan pingNodeStatus {
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
			wg.Done()
		}(node)
	}

	// Wait for all goroutines to finish, then close channel "done" so that exptWorker can proceed
	go func() {
		wg.Wait()
		close(done)
	}()

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
					}
					c <- cps
				}(acct, role, node)
			}
		}(acct, role)
	}
	return c
}

func copyLogs(exptSuccess bool, exptlogpath, exptgenlogpath string, logconfig map[string]string) {
	copyLog := func(src, dest string) {
		data, err := ioutil.ReadFile(src)
		if err != nil {
			if exptSuccess { // If the experiment was successful, there would be no error logfile, so we're not worried
				return
			}
			log.Error("Could not read experiment logfile ", src)
			return
		}

		rwmux.Lock()
		f, err := os.OpenFile(dest, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Error(err)
			log.Error("Please clean up manually")
		} else {
			if _, err = f.Write(data); err != nil {
				log.Error(err)
				log.Error("Please clean up manually")
			}
			if err = f.Close(); err != nil {
				log.Error(err)
				log.Errorf("Could not close file %s.  Please investigate", f.Name())
			}
		}
		rwmux.Unlock()

		if err := os.Remove(src); err != nil {
			log.Errorf("Could not remove experiment log %s.  Please clean up manually", src)
		}
	}

	copyLog(exptgenlogpath, viper.GetString("logs.logfile"))
	copyLog(exptlogpath, viper.GetString("logs.errfile"))

}

func (expt *experimentSuccess) experimentCleanup() error {
	exptlogfilename := fmt.Sprintf(exptLogFilename, expt.name)

	dir, err := os.Getwd()
	if err != nil {
		return errors.New(`Could not get current working directory.  Aborting cleanup.  
					Please check working directory and manually clean up log files`)
	}

	exptlogfilepath := path.Join(dir, fmt.Sprintf(exptLogFilename, expt.name))
	exptgenlogfilepath := path.Join(dir, fmt.Sprintf(exptGenFilename, expt.name))

	defer copyLogs(expt.success, exptlogfilepath, exptgenlogfilepath, viper.GetStringMapString("logs"))

	// No experiment logfile
	if _, err = os.Stat(exptlogfilepath); os.IsNotExist(err) {
		return nil
	}

	if !expt.success {
		// Try to send email, which also deletes expt file, returns error
		// var err error = nil // Dummy
		// err := sendEmail(expt.name, exptlogfilepath, emailSlice)
		// err := errors.New("Dummy error for email") // Take this line out and replace it with
		if testMode {
			return nil // Don't do anything - we're testing.
		}
		data, err := ioutil.ReadFile(exptlogfilepath)
		if err != nil {
			return err
		}

		msg := string(data)

		if err := sendEmail(expt.name, msg); err != nil {
			newfilename := fmt.Sprintf("%s-%s", exptlogfilename, time.Now().Format(time.RFC3339))
			newpath := path.Join(dir, newfilename)

			if err := os.Rename(exptlogfilepath, newpath); err != nil {
				return fmt.Errorf("Could not move file %s to %s.  The error was %v", exptlogfilepath, newpath, err)
			}
			return fmt.Errorf("Could not send email for experiment %s.  Archived error file at %s", expt.name, newpath)
		}
	}

	return nil
}

func experimentWorker(exptname string, w *sync.WaitGroup, done <-chan bool) <-chan experimentSuccess {
	c := make(chan experimentSuccess)
	expt := experimentSuccess{exptname, true} // Initialize
	exptLog := exptLogInit(expt.name)
	exptLog.Info("Now processing ", expt.name)

	go func() {
		exptConfig := viper.Sub("experiments." + expt.name)

		badnodes := make(map[string]struct{})
		successfulCopies := make(map[string][]string)

		declareExptFailure := func() {
			expt.success = false
			c <- expt
			close(c)
		}

		for _, node := range exptConfig.GetStringSlice("nodes") {
			badnodes[node] = struct{}{}
		}

		// if e == "darkside" {
		// 	time.Sleep(20 * time.Second)
		// }

		if !viper.IsSet("global.krb5ccname") {
			exptLog.Error(`Could not obtain KRB5CCNAME environmental variable from
				config.  Please check the config file on fifeutilgpvm01.`)
			declareExptFailure()
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
			declareExptFailure()
			return
		}

		var pingWG sync.WaitGroup
		pingWG.Add(len(exptConfig.GetStringSlice("nodes")))
		pingDone := make(chan struct{})
		pingChannel := pingAllNodes(exptConfig.GetStringSlice("nodes"), &pingWG, pingDone)
		for {
			select {
			case <-pingDone: // pingDone is closed
				break
			case testnode := <-pingChannel: // Receive on pingChannel
				if testnode.err == nil {
					delete(badnodes, testnode.node)
				} else {
					exptLog.Error(testnode.err)
				}
			case <-time.After(pingTimeoutDuration): // We give pingTimeoutDuration for each receive
				exptLog.Error("Hit the ping timeout!")
				break
			}
		}

		// for _ = range exptConfig.GetStringSlice("nodes") { // Note that we're iterating over the range of nodes so we make sure
		// 	// that we listen on the channel the right number of times
		// 	select {
		// 	case testnode := <-pingChannel:
		// 		if testnode.err == nil {
		// 			delete(badnodes, testnode.node)
		// 		} else {
		// 			exptLog.Error(testnode.err)
		// 		}
		// 	case <-time.After(pingTimeoutDuration):
		// 	}
		// }

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
			case <-time.After(vpiTimeoutDuration):
				exptLog.Errorf("Error obtaining proxy for %s:  timeout.  Check log for details. Continuing to next proxy.\n", expt.name)
				expt.success = false
			}
		}

		copyChan := copyProxies(exptConfig)
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
				case <-time.After(exptTimeoutDuration):
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
		if err := expt.experimentCleanup(); err != nil {
			log.Error(err)
		}
		log.Info("Finished cleaning up ", expt.name)

		select {
		case <-done:
			w.Done() // Decrement WaitGroup here so that expt manager doesn't close agg channel
			// before expt cleanup is done
		case <-time.After(exptTimeoutDuration):
			log.Error("Timed out waiting for experiment success info to be put into aggregation channel")
			w.Done()
		}
	}()
	return c
}

// Global functions

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
	var wg sync.WaitGroup
	wg.Add(len(exptList))

	// Start all of the experiment workers, put their results into the agg channel
	for _, expt := range exptList {
		go func(expt string) {
			done := make(chan bool) // Channel to send signal to expt worker that we've put its
			// result into agg channel, so it can return after cleanup is done
			c := experimentWorker(expt, &wg, done)
			agg <- <-c
			close(done)
		}(expt)
	}

	// This will wait until all expt workers have put their values into agg channel, and have finished
	// cleanup.  This prevents the rare race condition that main() returns before all expt cleanup
	// is done, since main() waits for the agg channel to close before doing cleanup.
	go func() {
		wg.Wait()
		log.Debug("Closing aggregation channel")
		close(agg)
	}()

	return agg
}

func sendEmail(exptName, message string) error {
	var recipients []string

	if exptName == "" {
		exptName = "all experiments" // Send email for all experiments to admin
		recipients = viper.GetStringSlice("notifications.admin_email")
	} else {
		emailsKeyLookup := "experiments." + exptName + ".emails"
		recipients = viper.GetStringSlice(emailsKeyLookup)
	}

	subject := "Managed Proxy Push errors for " + exptName

	m := gomail.NewMessage()
	m.SetHeader("From", "fife-group@fnal.gov")
	m.SetHeader("To", recipients...)
	m.SetHeader("Subject", subject)
	m.SetBody("text/plain", message)

	err := emailDialer.DialAndSend(m)
	return err

}

func sendSlackMessage(message string) error {
	msg := []byte(fmt.Sprintf(`{"text": "%s"}`, strings.Replace(message, "\"", "\\\"", -1)))
	req, err := http.NewRequest("POST", viper.GetString("notifications.slack_alerts_url"), bytes.NewBuffer(msg))
	req.Header.Set("Content-Type", "application/json")

	slackTimeoutDuration, err := time.ParseDuration(slackTimeout)
	if err != nil {
		return errors.New("Invalid slack timeout string")
	}
	client := &http.Client{Timeout: slackTimeoutDuration}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := ioutil.ReadAll(resp.Body)
		errmsg := fmt.Errorf("Slack Response Status: %s\nSlack Response Headers: %s\nSlack Response Body: %s",
			resp.Status, resp.Header, string(body))
		return errmsg
	}
	fmt.Println("Slack message sent")
	return nil
}

func init() {
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
	testMode = viper.GetBool("test")
	if testMode {
		log.Info("Running in test mode")
		viper.Set("notifications", viper.Get("notifications_test"))
	}

	// Now that our log is set up and we've got a valid config, handle all init (fatal) errors using the following func
	// that logs the error, sends a slack message and an email, cleans up, and then exits.
	initErrorNotify := func(m string) {
		log.Error(m)
		sendSlackMessage(m)
		sendEmail("", m)

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

	// Check our timeouts for proper formatting
	globalTimeoutDuration, err = time.ParseDuration(globalTimeout)
	if err != nil {
		msg := fmt.Sprintf("Invalid global timeout string %s", globalTimeout)
		initErrorNotify(msg)
	}

	exptTimeoutDuration, err = time.ParseDuration(exptTimeout)
	if err != nil {
		msg := fmt.Sprintf("Invalid experiment timeout string %s", exptTimeout)
		initErrorNotify(msg)
	}

	pingTimeoutDuration, err = time.ParseDuration(pingTimeout)
	if err != nil {
		msg := fmt.Sprintf("Invalid ping timeout string %s", pingTimeout)
		initErrorNotify(msg)
	}

	vpiTimeoutDuration, err = time.ParseDuration(vpiTimeout)
	if err != nil {
		msg := fmt.Sprintf("Invalid voms-proxy-init timeout string %s", vpiTimeout)
		initErrorNotify(msg)
	}

}

func cleanup(exptStatus map[string]bool, experiments []string) error {
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

	if _, err := os.Stat(viper.GetString("logs.errfile")); os.IsNotExist(err) {
		log.Info("Proxy Push completed with no errors")
		return nil
	}

	data, err := ioutil.ReadFile(viper.GetString("logs.errfile"))
	if err != nil {
		return err
	}

	msg := string(data)

	finalCleanupSuccess := true
	err = sendEmail("", msg)
	if err != nil {
		log.Error(err)
		finalCleanupSuccess = false
	}
	err = sendSlackMessage(msg)
	if err != nil {
		log.Error(err)
		finalCleanupSuccess = false
	}

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
	// go func() {
	// 	log.Println(http.ListenAndServe("localhost:6060", nil))
	// }()
	// runtime.SetBlockProfileRate(1)

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
	c := manageExperimentChannels(expts)
	// Listen on the manager channel
	timeout := time.After(globalTimeoutDuration)
	for {
		select {
		case expt, chanOK := <-c:
			if !chanOK {
				err := cleanup(exptSuccesses, expts)
				if err != nil {
					log.Error(err)
				}
				return
			}
			exptSuccesses[expt.name] = expt.success
		case <-timeout:
			log.Error("Hit the global timeout!")
			err := cleanup(exptSuccesses, expts)
			if err != nil {
				log.Error(err)
			}
			return
		}
	}
}
