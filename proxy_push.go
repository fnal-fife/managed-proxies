package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	"os/user"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/rifflock/lfshook"
	"github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	gomail "gopkg.in/gomail.v2"
)

//notifications - IN PROGRESS - Formatting

// Error handling - break everything!

// Timeouts, defaults, and format strings

const (
	configFile      string = "proxy_push_config_test.yml"       // CHANGE ME BEFORE PRODUCTION
	exptLogFilename string = "golang_proxy_push_%s.log"         // CHANGE ME BEFORE PRODUCTION - temp file per experiment that will be emailed to experiment
	exptGenFilename string = "golang_proxy_push_general_%s.log" // CHANGE ME BEFORE PRODUCTION - temp file per experiment that will be copied over to logfile
)

var timeoutStrings = map[string]string{
						"globalTimeout": "60s", // Global timeout
						"exptTimeout":   "30s", // Experiment timeout
						"pingTimeout":   "10s", // Ping timeout (for all pings per experiment)
						"vpiTimeout":    "10s", // voms-proxy-init timeout (total for vpis per experiment)
						"copyTimeout":   "30s", // copy proxy timeout (total for all copies per experiment)
						"slackTimeout":  "15s", // Slack message timeout
						"emailTimeout":  "30s"} // Email timeout (per email)
var timeoutDurationMap map[string]time.Duration // Hold the durations for code to use

var log = logrus.New()                                       // Global logger
var emailDialer = gomail.Dialer{Host: "localhost", Port: 25} // gomail dialer to use to send emails
var rwmuxErr, rwmuxLog sync.RWMutex                          // mutexes to be used when copying experiment logs into master and error log
// if testMode is true:  send only general emails to notifications_test.admin_email in config file, send Slack notification to test channel. Do not send experiment-specific email
var testMode bool

// Types to carry information about success and status of various operations over channels

// experimentSuccess stores information on whether all the processes involved in generating, copying, and changing permissions on all proxies for
// an experiment were successful.
type experimentSuccess struct {
	name    string
	success bool
}

// pingNodeStatus stores information about an attempt to ping a node.  If there was an error, it's stored in err.
type pingNodeStatus struct {
	node string
	err  error
}

// vomsProxyStatus stores information about an attempt to run voms-proxy-init to generate a VOMS proxy.
// If there was an error, it's stored in err.
type vomsProxyStatus struct {
	filename string
	err      error
}

// copyProxiesStatus stores information that uniquely identifies a VOMS proxy within an experiment (account, role) and the node
// to which it was attempted to be copied.  If there was an error doing so, it's stored in err.
type copyProxiesStatus struct {
	node    string
	account string
	role    string
	err     error
}

// Experiment worker-specific functions

// exptLogInit sets up the logrus instance for the experiment worker
// It returns a pointer to a logrus.Entry object that can be used to log events.
func exptLogInit(ctx context.Context, ename string) (*logrus.Entry, error) {
	if e := ctx.Err(); e != nil {
		return nil, e
	}

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

	exptlogger := Log.WithField("experiment", ename)
	exptlogger.Info("Set up experiment logger")

	return exptlogger, nil

}

// getKerbTicket runs kinit to get a kerberos ticket
func getKerbTicket(ctx context.Context, krb5ccname string) error {
	os.Setenv("KRB5CCNAME", krb5ccname)

	kerbcmdargs := []string{"-k", "-t",
		"/opt/gen_keytabs/config/gcso_monitor.keytab",
		"monitor/gcso/fermigrid.fnal.gov@FNAL.GOV"}

	cmd := exec.CommandContext(ctx, "/usr/krb5/bin/kinit", kerbcmdargs...)
	if cmdOut, cmdErr := cmd.CombinedOutput(); cmdErr != nil {
		if ctx.Err() == context.DeadlineExceeded {
			return ctx.Err()
		}
		return fmt.Errorf("Initializing a kerb ticket failed.  The error was %s: %s", cmdErr, cmdOut)
	}
	return nil
}

// checkKeys looks at the portion of the configuration passed in and makes sure the required keys are present
func checkKeys(ctx context.Context, exptConfig *viper.Viper) error {
	if e := ctx.Err(); e != nil {
		return e
	}

	if !exptConfig.IsSet("nodes") || !exptConfig.IsSet("accounts") {
		return errors.New(`Input file improperly formatted for %s (accounts or nodes don't 
			exist for this experiment). Please check the config file on fifeutilgpvm01.
			 I will skip this experiment for now`)
	}
	return nil
}

// pingAllNodes will launch goroutines, which each ping a node in the slice nodes.  It returns a channel,
// on which it reports the pingNodeStatuses signifying success or error
func pingAllNodes(ctx context.Context, nodes []string) <-chan pingNodeStatus {
	// Buffered Channel to report on
	c := make(chan pingNodeStatus, len(nodes))

	var wg sync.WaitGroup
	wg.Add(len(nodes))

	for _, node := range nodes {
		go func(node string) {
			defer wg.Done()
			p := pingNodeStatus{node, nil}
			pingargs := []string{"-W", "5", "-c", "1", node}
			cmd := exec.CommandContext(ctx, "ping", pingargs...)
			if cmdOut, cmdErr := cmd.CombinedOutput(); cmdErr != nil {
				if e := ctx.Err(); e != nil {
					p.err = e
				} else {
					p.err = fmt.Errorf("%s %s", cmdErr, cmdOut)
				}
			}
			c <- p
		}(node)
	}

	// Wait for all goroutines to finish, then close channel so that exptWorker can proceed
	go func() {
		defer close(c)
		wg.Wait()
	}()

	return c
}

// getProxies launches goroutines that run voms-proxy-init to generate the appropriate proxies on the local machine.
// Calling getProxies will generate all of the proxies per experiment according to the viper configuration.  It returns
// a channel, on which it reports the status of each attempt
func getProxies(ctx context.Context, exptConfig *viper.Viper, globalConfig map[string]string, exptname string) <-chan vomsProxyStatus {
	c := make(chan vomsProxyStatus, len(exptConfig.GetStringMapString("accounts")))
	var vomsprefix, certfile, keyfile string
	var wg sync.WaitGroup
	wg.Add(len(exptConfig.GetStringMapString("accounts")))

	if exptConfig.IsSet("vomsgroup") {
		vomsprefix = exptConfig.GetString("vomsgroup")
	} else {
		vomsprefix = "fermilab:/fermilab/" + exptname + "/"
	}

	for account, role := range exptConfig.GetStringMapString("accounts") {
		go func(account, role string) {
			defer wg.Done()
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

			cmd := exec.CommandContext(ctx, "/usr/bin/voms-proxy-init", vpiargs...)
			if cmdErr := cmd.Run(); cmdErr != nil {
				if e := ctx.Err(); e != nil {
					vpi.err = e
				} else {
					err := fmt.Sprintf(`Error obtaining %s.  Please check the cert on 
					fifeutilgpvm01. \n%s Continuing on to next role.`, outfile, cmdErr)
					vpi.err = errors.New(err)
				}
			}
			// if e == "darkside" {
			// 	time.Sleep(time.Duration(10) * time.Second)
			// }

			c <- vpi
		}(account, role)
	}

	// Wait for all goroutines to finish, then close channel so that exptWorker can proceed
	go func() {
		defer close(c)
		wg.Wait()
	}()

	return c
}

// copyProxies copies the proxies from the local machine to the experiment nodes as specified by the configuration and changes their permissions.
// It returns a channel on which it reports the status of those operations.  The copy and change permission operations share a context that
// dictates their deadline.
func copyProxies(ctx context.Context, exptConfig *viper.Viper) <-chan copyProxiesStatus {
	numSlots := len(exptConfig.GetStringMapString("accounts")) * len(exptConfig.GetStringSlice("nodes"))
	c := make(chan copyProxiesStatus, numSlots)
	var wg sync.WaitGroup
	wg.Add(numSlots)

	// One copy per node and role
	for acct, role := range exptConfig.GetStringMapString("accounts") {
		go func(acct, role string) {
			proxyFile := acct + "." + role + ".proxy"
			proxyFilePath := path.Join("proxies", proxyFile)

			for _, node := range exptConfig.GetStringSlice("nodes") {
				go func(node string) {
					defer wg.Done()
					cps := copyProxiesStatus{node, acct, role, nil}
					accountNode := acct + "@" + node + ".fnal.gov"
					newProxyPath := path.Join(exptConfig.GetString("dir"), acct, proxyFile+".new")
					finalProxyPath := path.Join(exptConfig.GetString("dir"), acct, proxyFile)

					sshopts := []string{"-o", "ConnectTimeout=30",
						"-o", "ServerAliveInterval=30",
						"-o", "ServerAliveCountMax=1"}

					scpargs := append(sshopts, proxyFilePath, accountNode+":"+newProxyPath)
					sshargs := append(sshopts, accountNode, "chmod 400 "+newProxyPath+" ; mv -f "+newProxyPath+" "+finalProxyPath)
					scpCmd := exec.CommandContext(ctx, "scp", scpargs...)
					sshCmd := exec.CommandContext(ctx, "ssh", sshargs...)

					if cmdOut, cmdErr := scpCmd.CombinedOutput(); cmdErr != nil {
						if e := ctx.Err(); e != nil {
							cps.err = e
						} else {
							cps.err = fmt.Errorf("Copying proxy %s to node %s failed.  The error was %s: %s", proxyFile, node, cmdErr, cmdOut)
						}
						c <- cps
						return
					}

					if cmdOut, cmdErr := sshCmd.CombinedOutput(); cmdErr != nil {
						if e := ctx.Err(); e != nil {
							cps.err = e
						} else {
							cps.err = fmt.Errorf("Error changing permission of proxy %s to mode 400 on %s.  The error was %s: %s", proxyFile, node, cmdErr, cmdOut)
						}
					}
					c <- cps
				}(node)
			}
		}(acct, role)
	}

	// Wait for all goroutines to finish, then close channel so that exptWorker can proceed
	go func() {
		defer close(c)
		wg.Wait()
	}()

	return c
}

// copyLogs copies the experiment-specific logs to the general and error logs.
func copyLogs(ctx context.Context, exptSuccess bool, exptlogpath, exptgenlogpath string, logconfig map[string]string) {
	if e := ctx.Err(); e != nil {
		log.Error(e)
		return
	}

	var wg sync.WaitGroup
	wg.Add(2)

	copyLog := func(src, dest string, rwmux *sync.RWMutex) {
		defer wg.Done()

		if e := ctx.Err(); e != nil {
			log.Error(e)
			return
		}

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

	go copyLog(exptgenlogpath, viper.GetString("logs.logfile"), &rwmuxLog)
	go copyLog(exptlogpath, viper.GetString("logs.errfile"), &rwmuxErr)
	wg.Wait()
}

// experimentCleanup manages the cleanup operations for an experiment, such as sending emails if necessary, and copying, removing or archiving the logs
func (expt *experimentSuccess) experimentCleanup(ctx context.Context) error {
	if e := ctx.Err(); e != nil {
		return e
	}

	exptlogfilename := fmt.Sprintf(exptLogFilename, expt.name)

	dir, err := os.Getwd()
	if err != nil {
		return errors.New(`Could not get current working directory.  Aborting cleanup.  
					Please check working directory and manually clean up log files`)
	}

	exptlogfilepath := path.Join(dir, fmt.Sprintf(exptLogFilename, expt.name))
	exptgenlogfilepath := path.Join(dir, fmt.Sprintf(exptGenFilename, expt.name))

	defer copyLogs(ctx, expt.success, exptlogfilepath, exptgenlogfilepath, viper.GetStringMapString("logs"))

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

		emailCtx, emailCancel := context.WithTimeout(ctx, timeoutDurationMap["emailTimeout"])
		defer emailCancel()
		if err := sendEmail(emailCtx, expt.name, msg); err != nil {
			newfilename := fmt.Sprintf("%s-%s", exptlogfilename, time.Now().Format(time.RFC3339))
			newpath := path.Join(dir, newfilename)

			if err := os.Rename(exptlogfilepath, newpath); err != nil {
				return fmt.Errorf("Could not move file %s to %s.  The error was %v", exptlogfilepath, newpath, err)
			}
			return fmt.Errorf("Could not send email for experiment %s.  Archived error file at %s.  The error was %v", expt.name, newpath, err)
		}
	}

	return nil
}

// experimentWorker is the main func that manages the processes involved in generating and copying VOMS proxies to an experiment's nodes.  It returns a channel
// on which it reports the status of that experiment's proxy push
func experimentWorker(ctx context.Context, exptname string) <-chan experimentSuccess {
	var exptLog *logrus.Entry
	c := make(chan experimentSuccess, 2)
	expt := experimentSuccess{exptname, true} // Initialize

	exptLog, err := exptLogInit(ctx, expt.name)
	if err != nil { // We either have panicked or it's a context error.  If it's the latter, we really don't care
		exptLog = log.WithField("experiment", exptname)
	}

	exptLog.Info("Now processing ", expt.name)

	go func() {
		// defer w.Done() // Decrement WaitGroup after cleanup is done or we want to return
		defer close(c) // All expt operations are done (either successful including cleanup or at error)
		exptConfig := viper.Sub("experiments." + expt.name)

		// badnodes := make(map[string]struct{})
		successfulCopies := make(map[string][]string)

		// Helper functions
		declareExptFailure := func() {
			// defer close(c)
			expt.success = false
			c <- expt
			// close(c)
		}

		// for _, node := range exptConfig.GetStringSlice("nodes") {
		// 	badnodes[node] = struct{}{}
		// }
		badNodesSlice := make([]string, 0, len(exptConfig.GetStringSlice("nodes")))

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
		if err := getKerbTicket(ctx, krb5ccnameCfg); err != nil {
			exptLog.Error(err)
		}

		// If check of exptConfig keys fails, experiment fails immediately
		if err := checkKeys(ctx, exptConfig); err != nil {
			exptLog.Error(err)
			declareExptFailure()
			return
		}

		pingCtx, pingCancel := context.WithTimeout(ctx, timeoutDurationMap["pingTimeout"])
		pingChannel := pingAllNodes(pingCtx, exptConfig.GetStringSlice("nodes"))
		// timeout := time.After(pingTimeoutDuration)
		// Listen until we either timeout or the pingChannel is closed
	pingLoop:
		for {
			select {
			case testnode, chanOpen := <-pingChannel: // Receive on pingChannel
				if !chanOpen { // Break out of loop and proceed only if channel is not open
					pingCancel()
					break pingLoop
				}
				if testnode.err != nil {
					badNodesSlice = append(badNodesSlice, testnode.node)
					// 	// delete(badnodes, testnode.node)
					// } else {
					exptLog.Error(testnode.err)
				}

				// if testnode.err == nil {
				// 	delete(badnodes, testnode.node)
				// } else {
				// 	exptLog.Error(testnode.err)
				// }
			case <-pingCtx.Done():
				if e := pingCtx.Err(); e == context.DeadlineExceeded {
					exptLog.Errorf("Hit the ping timeout: %s", e)

				} else {
					exptLog.Error(e)
				}
				pingCancel()
				break pingLoop
			}
			// case <-timeout: // We give timeout for all pings
			// 	exptLog.Error("Hit the ping timeout!")
			// 	break pingLoop
			// }
		}

		// badNodesSlice := make([]string, 0, len(badnodes))
		// for node := range badnodes {
		// 	badNodesSlice = append(badNodesSlice, node)
		// }

		if len(badNodesSlice) > 0 {
			exptLog.Warn("Bad nodes are: ", badNodesSlice)
		}

		// If voms-proxy-init fails, we'll just continue on.  We'll still try to push proxies,
		// since they're valid for 24 hours
		vpiCtx, vpiCancel := context.WithTimeout(ctx, timeoutDurationMap["vpiTimeout"])
		vpiChan := getProxies(vpiCtx, exptConfig, viper.GetStringMapString("global"), expt.name)
		// timeout := time.After(vpiTimeoutDuration)
		// Listen until we either timeout or vpiChan is closed
	vpiLoop:
		for {
			select {
			case vpi, chanOpen := <-vpiChan: // receive on vpiChan
				if !chanOpen {
					vpiCancel()
					break vpiLoop
				}
				if vpi.err != nil {
					exptLog.Error(vpi.err)
					expt.success = false
				} else {
					exptLog.Debug("Generated voms proxy: ", vpi.filename)
				}
			case <-vpiCtx.Done():
				if e := vpiCtx.Err(); e == context.DeadlineExceeded {
					exptLog.Errorf("Hit the voms-proxy-init timeout for %s: %s.  Check log for details.  Continuing to next proxy.\n", expt.name, e)
				} else {
					exptLog.Error(e)
				}
				vpiCancel()
				break vpiLoop
				// case <-timeout: // voms-proxy-init timeout for all operations
				// 	exptLog.Errorf("Error obtaining proxy for %s:  timeout.  Check log for details. Continuing to next proxy.\n", expt.name)
				// 	expt.success = false
				// 	break vpiLoop
			}
		}

		copyCtx, copyCancel := context.WithTimeout(ctx, timeoutDurationMap["copyTimeout"])
		copyChan := copyProxies(copyCtx, exptConfig)
		// timeout := time.After(exptTimeoutDuration)
		// Listen until we either timeout or the copyChan is closed
	copyLoop:
		for {
			select {
			case pushproxy, chanOpen := <-copyChan:
				if !chanOpen {
					copyCancel()
					break copyLoop
				}
				if pushproxy.err != nil {
					exptLog.Error(pushproxy.err)
					expt.success = false
				} else {
					successfulCopies[pushproxy.role] = append(successfulCopies[pushproxy.role], pushproxy.node)
				}
			case <-copyCtx.Done():
				if e := copyCtx.Err(); e == context.DeadlineExceeded {
					exptLog.Error("Experiment hit the timeout when waiting to push proxy.")
				} else {
					exptLog.Error(e)
				}
				expt.success = false
				copyCancel()
				break copyLoop
				// case <-timeout: // Copy proxy timeout for all proxies
				// 	exptLog.Error("Experiment hit the timeout when waiting to push proxy.")
				// 	expt.success = false
				// 	break copyLoop
			}
		}

		for role, nodes := range successfulCopies {
			sort.Strings(nodes)
			exptLog.Debugf("Successful copies for role %s were %v", role, nodes)
		}
		exptLog.Info("Finished processing ", expt.name)
		c <- expt
		// close(c)

		// We're logging the cleanup in the general log so that we don't create an extraneous
		// experiment log file
		if err := expt.experimentCleanup(ctx); err != nil {
			log.Error(err)
		}
		log.Info("Finished cleaning up ", expt.name)

		// // Block until we get go-ahead from expt manager that it's put message into agg channel or timeout expires
		// select {
		// case <-done:
		// case <-time.After(exptTimeoutDuration):
		// 	log.Error("Timed out waiting for experiment success info to be put into aggregation channel")
		// }
	}()
	return c
}

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

// manageExperimentChannels starts up the various experimentWorkers and listens for their response.  It puts these statuses into an aggregate channel.
func manageExperimentChannels(ctx context.Context, exptList []string) <-chan experimentSuccess {
	agg := make(chan experimentSuccess, len(exptList))
	var wg sync.WaitGroup
	wg.Add(len(exptList))

	// Start all of the experiment workers, put their results into the agg channel
	for _, expt := range exptList {
		go func(expt string) {
			exptContext, exptCancel := context.WithTimeout(ctx, timeoutDurationMap["exptTimeout"])
			defer wg.Done()
			defer exptCancel()

			// If all goes well, each experimentWorker channel will be ready to be received on twice:  once when the
			// successful status is sent, and when the channel closes after cleanup.  If we timeout, just move on.  Expt channel
			// is buffered anyway, so if the worker tries to send later and there's no receiver, garbage collection
			// will take care of it
			c := experimentWorker(exptContext, expt)
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
func sendEmail(ctx context.Context, exptName, message string) error {
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

	c := make(chan error)
	go func() {
		defer close(c)
		err := emailDialer.DialAndSend(m)
		c <- err
	}()

	select {
	case e := <-c:
		return e
	case <-ctx.Done():
		e := ctx.Err()
		if e != context.DeadlineExceeded {
			return fmt.Errorf("Hit timeout attempting to send email to %s", exptName)
		}
		return e
	}
}

// sendSlackMessage sends an HTTP POST request to a URL specified in the config file.
func sendSlackMessage(ctx context.Context, message string) error {
	if e := ctx.Err(); e != nil {
		return e
	}

	msg := []byte(fmt.Sprintf(`{"text": "%s"}`, strings.Replace(message, "\"", "\\\"", -1)))
	req, err := http.NewRequest("POST", viper.GetString("notifications.slack_alerts_url"), bytes.NewBuffer(msg))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: timeoutDurationMap["slackTimeout"]}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}

	// This should be redundant, but just in case the timeout before didn't trigger.
	if e := ctx.Err(); e != nil {
		return e
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

		// Durations are hard-coded here since we haven't parsed them out yet
		slackInitCtx, slackInitCancel := context.WithTimeout(context.Background(), time.Duration(15*time.Second))
		sendSlackMessage(slackInitCtx, m)
		slackInitCancel()

		emailInitCtx, emailInitCancel := context.WithTimeout(context.Background(), time.Duration(30*time.Second))
		sendEmail(emailInitCtx, "", m)
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
	timeoutDurationMap = make(map[string]time.Duration)

	for timeoutName, timeoutString := range timeoutStrings {
		value, err := time.ParseDuration(timeoutString)
		if err != nil {
			msg := fmt.Sprintf("Invalid %s value: %s", timeoutName, timeoutString)
			initErrorNotify(msg)
		}
		timeoutDurationMap[timeoutName] = value
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

	emailCtx, emailCancel := context.WithTimeout(context.Background(), timeoutDurationMap["emailTimeout"])
	if err = sendEmail(emailCtx, "", msg); err != nil {
		log.Error(err)
		finalCleanupSuccess = false
	}
	emailCancel()

	slackCtx, slackCancel := context.WithTimeout(context.Background(), timeoutDurationMap["slackTimeout"])
	if err = sendSlackMessage(slackCtx, msg); err != nil {
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
	ctx, cancel := context.WithTimeout(context.Background(), timeoutDurationMap["globalTimeout"])
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
			exptSuccesses[expt.name] = expt.success
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
