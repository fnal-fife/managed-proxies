package experimentutil

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"fnal.gov/sbhat/proxypush/notifications"
	"github.com/rifflock/lfshook"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

const (
	exptLogFilename string = "golang_proxy_push_%s.log"         // CHANGE ME BEFORE PRODUCTION - temp file per experiment that will be emailed to experiment
	exptGenFilename string = "golang_proxy_push_general_%s.log" // CHANGE ME BEFORE PRODUCTION - temp file per experiment that will be copied over to logfile
)

var genLog *logrus.Logger
var rwmuxErr, rwmuxLog sync.RWMutex // mutexes to be used when copying experiment logs into master and error log

// Types to carry information about success and status of various operations over channels

// ExperimentSuccess stores information on whether all the processes involved in generating, copying, and changing
// permissions on all proxies for an experiment were successful.
type ExperimentSuccess struct {
	Name    string
	Success bool
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

// copyProxiesStatus stores information that uniquely identifies a VOMS proxy within an experiment (account, role) and
// the node to which it was attempted to be copied.  If there was an error doing so, it's stored in err.
type copyProxiesStatus struct {
	node    string
	account string
	role    string
	err     error
}

// ExptErrorFormatter is a custom formatter that is intended to be used for the
// experiment error logs that will be sent out to experiments and to the admins.
// The default logrus.TextFormatter seems a little tougher to parse for a human.
type ExptErrorFormatter struct {
}

// Format defines how any logger using the ExptErrorFormatter should emit its
// log records.  We expect to see [date] [experiment] [level] [message]
func (f *ExptErrorFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	var expt string
	if val, ok := entry.Data["experiment"]; ok {
		if expt, ok = val.(string); !ok {
			return nil, errors.New("entry.Data[\"experiment\"] Failed type assertion")
		}
	} else {
		expt = "No Experiment"
	}

	logLine := fmt.Sprintf("[%s] [%s] [%s]: %s", entry.Time, expt, entry.Level, entry.Message)
	logByte := []byte(logLine)
	return append(logByte, '\n'), nil
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
	// logFormatter := logrus.TextFormatter{FullTimestamp: true}
	// Log.Formatter = &logFormatter

	// General Log that gets copied to master log
	Log.AddHook(lfshook.NewHook(lfshook.PathMap{
		logrus.DebugLevel: genlog,
		logrus.InfoLevel:  genlog,
		logrus.WarnLevel:  genlog,
		logrus.ErrorLevel: genlog,
		logrus.FatalLevel: genlog,
		logrus.PanicLevel: genlog,
	}, &logrus.TextFormatter{FullTimestamp: true}))

	// Experiment-specific error log that gets emailed if populated
	Log.AddHook(lfshook.NewHook(lfshook.PathMap{
		logrus.ErrorLevel: exptlog,
		logrus.FatalLevel: exptlog,
		logrus.PanicLevel: exptlog,
	}, new(ExptErrorFormatter)))

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
		return fmt.Errorf("Obtaining a kerberos ticket failed.  May be unable to "+
			"push proxies.  The error was %s: %s", cmdErr, cmdOut)
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
func getProxies(ctx context.Context, exptConfig *viper.Viper, globalConfig map[string]string,
	exptname string) <-chan vomsProxyStatus {

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

// copyProxies copies the proxies from the local machine to the experiment nodes as specified by the configuration and
// changes their permissions. It returns a channel on which it reports the status of those operations.  The copy and
// change permission operations share a context that  dictates their deadline.
func copyProxies(ctx context.Context, exptConfig *viper.Viper, badNodesSlice []string) <-chan copyProxiesStatus {
	numSlots := len(exptConfig.GetStringMapString("accounts")) * len(exptConfig.GetStringSlice("nodes"))
	badNodesMap := make(map[string]struct{})
	c := make(chan copyProxiesStatus, numSlots)
	var wg sync.WaitGroup

	for _, node := range badNodesSlice {
		badNodesMap[node] = struct{}{}
	}

	wg.Add(numSlots)

	// One copy per node and role
	for acct, role := range exptConfig.GetStringMapString("accounts") {
		go func(acct, role string) {
			proxyFile := acct + "." + role + ".proxy"
			proxyFilePath := path.Join("proxies", proxyFile)

			for _, node := range exptConfig.GetStringSlice("nodes") {
				go func(node string) {
					var badNodeMsg string
					defer wg.Done()
					cps := copyProxiesStatus{node, acct, role, nil}
					accountNode := acct + "@" + node + ".fnal.gov"
					newProxyPath := path.Join(exptConfig.GetString("dir"), acct, proxyFile+".new")
					finalProxyPath := path.Join(exptConfig.GetString("dir"), acct, proxyFile)

					if _, ok := badNodesMap[node]; ok {
						badNodeMsg = "\n" + fmt.Sprintf("Node %s didn't respond to pings earlier - "+
							"so it's expected that copying there would fail. "+
							"It may be necessary for the experiment to request via a "+
							"ServiceNow ticket that the Scientific Server Infrastructure "+
							"group reboot the node.", node)
					} else {
						badNodeMsg = ""
					}

					sshopts := []string{"-o", "ConnectTimeout=30",
						"-o", "ServerAliveInterval=30",
						"-o", "ServerAliveCountMax=1"}

					scpargs := append(sshopts, proxyFilePath, accountNode+":"+newProxyPath)
					sshargs := append(sshopts, accountNode,
						"chmod 400 "+newProxyPath+" ; mv -f "+newProxyPath+" "+finalProxyPath)
					scpCmd := exec.CommandContext(ctx, "scp", scpargs...)
					sshCmd := exec.CommandContext(ctx, "ssh", sshargs...)

					if cmdOut, cmdErr := scpCmd.CombinedOutput(); cmdErr != nil {
						if e := ctx.Err(); e != nil {
							cps.err = e
						} else {
							e := fmt.Sprintf("Copying proxy %s to node %s failed.  The error was %s: %s%s",
								proxyFile, node, cmdErr, cmdOut, badNodeMsg)
							cps.err = errors.New(e)
						}
						c <- cps
						return
					}

					if cmdOut, cmdErr := sshCmd.CombinedOutput(); cmdErr != nil {
						if e := ctx.Err(); e != nil {
							cps.err = e
						} else {
							cps.err = fmt.Errorf("Error changing permission of proxy %s to mode 400 on %s. "+
								"The error was %s: %s.%s", proxyFile, node, cmdErr, cmdOut, badNodeMsg)
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
		genLog.Error(e)
		return
	}

	var wg sync.WaitGroup
	wg.Add(2)

	copyLog := func(src, dest string, rwmux *sync.RWMutex) {
		defer wg.Done()

		if e := ctx.Err(); e != nil {
			genLog.Error(e)
			return
		}

		data, err := ioutil.ReadFile(src)
		if err != nil {
			if exptSuccess { // If the experiment was successful, there would be no error logfile, so we're not worried
				return
			}
			genLog.Error("Could not read experiment logfile ", src)
			return
		}

		rwmux.Lock()
		f, err := os.OpenFile(dest, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			genLog.Error(err)
			genLog.Error("Please clean up manually")
		} else {
			if _, err = f.Write(data); err != nil {
				genLog.Error(err)
				genLog.Error("Please clean up manually")
			}
			if err = f.Close(); err != nil {
				genLog.Error(err)
				genLog.Errorf("Could not close file %s.  Please investigate", f.Name())
			}
		}
		rwmux.Unlock()

		if err := os.Remove(src); err != nil {
			genLog.Errorf("Could not remove experiment log %s.  Please clean up manually", src)
		}
	}

	go copyLog(exptgenlogpath, viper.GetString("logs.logfile"), &rwmuxLog)
	go copyLog(exptlogpath, viper.GetString("logs.errfile"), &rwmuxErr)
	wg.Wait()
}

// experimentCleanup manages the cleanup operations for an experiment, such as sending emails if necessary,
// and copying, removing or archiving the logs
func (expt *ExperimentSuccess) experimentCleanup(ctx context.Context) error {
	if e := ctx.Err(); e != nil {
		return e
	}

	exptlogfilename := fmt.Sprintf(exptLogFilename, expt.Name)

	dir, err := os.Getwd()
	if err != nil {
		return errors.New(`Could not get current working directory.  Aborting cleanup.  
					Please check working directory and manually clean up log files`)
	}

	exptlogfilepath := path.Join(dir, fmt.Sprintf(exptLogFilename, expt.Name))
	exptgenlogfilepath := path.Join(dir, fmt.Sprintf(exptGenFilename, expt.Name))

	defer copyLogs(ctx, expt.Success, exptlogfilepath, exptgenlogfilepath, viper.GetStringMapString("logs"))

	// No experiment logfile
	if _, err = os.Stat(exptlogfilepath); os.IsNotExist(err) {
		return nil
	}

	if !expt.Success {
		// Try to send email, which also deletes expt file, returns error
		// var err error = nil // Dummy
		// err := sendEmail(expt.Name, exptlogfilepath, emailSlice)
		// err := errors.New("Dummy error for email") // Take this line out and replace it with
		if viper.GetBool("test") {
			return nil // Don't do anything - we're testing.
		}
		data, err := ioutil.ReadFile(exptlogfilepath)
		if err != nil {
			return err
		}

		msg := string(data)

		t, ok := viper.Get("emailTimeoutDuration").(time.Duration)
		if !ok {
			return errors.New("emailTimeoutDuration is not a time.Duration object")
		}
		emailCtx, emailCancel := context.WithTimeout(ctx, t)
		defer emailCancel()
		if err := notifications.SendEmail(emailCtx, expt.Name, msg); err != nil {
			newfilename := fmt.Sprintf("%s-%s", exptlogfilename, time.Now().Format(time.RFC3339))
			newpath := path.Join(dir, newfilename)

			if err := os.Rename(exptlogfilepath, newpath); err != nil {
				return fmt.Errorf("Could not move file %s to %s.  The error was %v", exptlogfilepath, newpath, err)
			}
			return fmt.Errorf("Could not send email for experiment %s.  Archived error file at %s. "+
				"The error was %v", expt.Name, newpath, err)
		}
	}
	return nil
}

// ExperimentWorker is the main func that manages the processes involved in generating and copying VOMS proxies to
// an experiment's nodes.  It returns a channel on which it reports the status of that experiment's proxy push.
// genlog is the logrus.logger that gets passed in that's meant to capture the non-experiment specific messages
// that might be printed from this module
func ExperimentWorker(ctx context.Context, exptname string, genLog *logrus.Logger) <-chan ExperimentSuccess {
	var exptLog *logrus.Entry
	c := make(chan ExperimentSuccess, 2)
	expt := ExperimentSuccess{exptname, true} // Initialize

	exptLog, err := exptLogInit(ctx, expt.Name)
	if err != nil { // We either have panicked or it's a context error.  If it's the latter, we really don't care here
		exptLog = genLog.WithField("experiment", exptname)
	}

	exptLog.Info("Now processing ", expt.Name)

	go func() {
		defer close(c) // All expt operations are done (either successful including cleanup or at error)
		exptConfig := viper.Sub("experiments." + expt.Name)
		successfulCopies := make(map[string][]string)
		badNodesSlice := make([]string, 0, len(exptConfig.GetStringSlice("nodes")))

		// Helper functions
		declareExptFailure := func() {
			expt.Success = false
			c <- expt
		}

		// if e == "darkside" {
		// 	time.Sleep(20 * time.Second)
		// }

		if !viper.IsSet("global.krb5ccname") {
			exptLog.Error("Could not obtain KRB5CCNAME environmental variable from config. " +
				"Please check the config file on fifeutilgpvm01.")
			declareExptFailure()
			return
		}
		krb5ccnameCfg := viper.GetString("global.krb5ccname")

		// If we can't get a kerb ticket, log error and keep going.
		// We might have an old one that's still valid.
		if err := getKerbTicket(ctx, krb5ccnameCfg); err != nil {
			exptLog.Warn(err)
		}

		// If check of exptConfig keys fails, experiment fails immediately
		if err := checkKeys(ctx, exptConfig); err != nil {
			exptLog.Error(err)
			declareExptFailure()
			return
		}
		exptLog.Debug("Config keys are valid")

		t, ok := viper.Get("pingTimeoutDuration").(time.Duration)
		if !ok {
			genLog.Error("pingTimeoutDuration is not a time.Duration object")
			declareExptFailure()
			return
		}
		pingCtx, pingCancel := context.WithTimeout(ctx, t)
		pingChannel := pingAllNodes(pingCtx, exptConfig.GetStringSlice("nodes"))
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
					exptLog.Error(testnode.err)
				}
			case <-pingCtx.Done():
				if e := pingCtx.Err(); e == context.DeadlineExceeded {
					exptLog.Errorf("Hit the ping timeout: %s", e)

				} else {
					exptLog.Error(e)
				}
				pingCancel()
				break pingLoop
			}
		}

		if len(badNodesSlice) > 0 {
			exptLog.Warnf("The node(s) %s didn't return a response to ping after 5 "+
				"seconds.  Please investigate, and see if the nodes are up. "+
				"We'll still try to copy proxies there.", strings.Join(badNodesSlice, ", "))
		}

		// If voms-proxy-init fails, we'll just continue on.  We'll still try to push proxies,
		// since they're valid for 24 hours
		t, ok = viper.Get("vpiTimeoutDuration").(time.Duration)
		if !ok {
			genLog.Error("vpiTimeoutDuration is not a time.Duration object")
			declareExptFailure()
			return
		}
		vpiCtx, vpiCancel := context.WithTimeout(ctx, t)
		vpiChan := getProxies(vpiCtx, exptConfig, viper.GetStringMapString("global"), expt.Name)
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
					expt.Success = false
				} else {
					exptLog.Debug("Generated voms proxy: ", vpi.filename)
				}
			case <-vpiCtx.Done():
				if e := vpiCtx.Err(); e == context.DeadlineExceeded {
					exptLog.Errorf("Hit the voms-proxy-init timeout for %s: %s.  Check log for details. "+
						"Continuing to next proxy.\n", expt.Name, e)
				} else {
					exptLog.Error(e)
				}
				vpiCancel()
				break vpiLoop
			}
		}

		t, ok = viper.Get("copyTimeoutDuration").(time.Duration)
		if !ok {
			genLog.Error("copyTimeoutDuration is not a time.Duration object")
			declareExptFailure()
			return
		}
		copyCtx, copyCancel := context.WithTimeout(ctx, t)
		copyChan := copyProxies(copyCtx, exptConfig, badNodesSlice)
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
					expt.Success = false
				} else {
					successfulCopies[pushproxy.role] = append(successfulCopies[pushproxy.role], pushproxy.node)
				}
			case <-copyCtx.Done():
				if e := copyCtx.Err(); e == context.DeadlineExceeded {
					exptLog.Error("Experiment hit the timeout when waiting to push proxy.")
				} else {
					exptLog.Error(e)
				}
				expt.Success = false
				copyCancel()
				break copyLoop
			}
		}

		for role, nodes := range successfulCopies {
			sort.Strings(nodes)
			exptLog.Debugf("Successful copies for role %s were %v", role, nodes)
		}
		exptLog.Info("Finished processing ", expt.Name)
		c <- expt

		// We're logging the cleanup in the general log so that we don't create an extraneous
		// experiment log file
		genLog.Info("Cleaning up ", expt.Name)
		if err := expt.experimentCleanup(ctx); err != nil {
			genLog.Error(err)
		}
		genLog.Info("Finished cleaning up ", expt.Name)
	}()
	return c
}
