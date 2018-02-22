// Package experimentutil contains all of the operations needed to push a VOMS X509 proxy as a part of the USDC Managed Proxy service that are
// experiment-specific.
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

const exptErrFilenamef string = "golang_proxy_push_%s.log" // CHANGE ME BEFORE PRODUCTION - temp file per experiment that will be emailed to experiment

var genLog *logrus.Logger
var rwmuxErr, rwmuxLog, rwmuxDebug sync.RWMutex // mutexes to be used when copying experiment logs into master and error log

// ExperimentSuccess stores information on whether all the processes involved in generating, copying, and changing
// permissions on all proxies for an experiment were successful.
type ExperimentSuccess struct {
	Name    string
	Success bool
}

type ExptErrorFormatter struct {
}

// Experiment worker-specific functions

// Setup and cleanup

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

// exptLogInit sets up the logrus instance for the experiment worker
// It returns a pointer to a logrus.Entry object that can be used to log events.
func exptLogInit(ctx context.Context, ename string) (*logrus.Entry, error) {
	if e := ctx.Err(); e != nil {
		return nil, e
	}

	var Log = logrus.New()
	expterrlog := fmt.Sprintf(exptErrFilenamef, ename)

	Log.SetLevel(logrus.DebugLevel)

	Log.AddHook(lfshook.NewHook(lfshook.PathMap{
		logrus.DebugLevel: viper.GetString("logs.debugfile"),
		logrus.InfoLevel:  viper.GetString("logs.debugfile"),
		logrus.WarnLevel:  viper.GetString("logs.debugfile"),
		logrus.ErrorLevel: viper.GetString("logs.debugfile"),
		logrus.FatalLevel: viper.GetString("logs.debugfile"),
		logrus.PanicLevel: viper.GetString("logs.debugfile"),
	}, &logrus.TextFormatter{FullTimestamp: true}))

	Log.AddHook(lfshook.NewHook(lfshook.PathMap{
		logrus.InfoLevel:  viper.GetString("logs.logfile"),
		logrus.WarnLevel:  viper.GetString("logs.logfile"),
		logrus.ErrorLevel: viper.GetString("logs.logfile"),
		logrus.FatalLevel: viper.GetString("logs.logfile"),
		logrus.PanicLevel: viper.GetString("logs.logfile"),
	}, &logrus.TextFormatter{FullTimestamp: true}))

	// General Error Log that will get sent to Admins
	Log.AddHook(lfshook.NewHook(lfshook.PathMap{
		logrus.ErrorLevel: viper.GetString("logs.errfile"),
		logrus.FatalLevel: viper.GetString("logs.errfile"),
		logrus.PanicLevel: viper.GetString("logs.errfile"),
	}, new(ExptErrorFormatter)))

	// Experiment-specific error log that gets emailed if populated
	Log.AddHook(lfshook.NewHook(lfshook.PathMap{
		logrus.ErrorLevel: expterrlog,
		logrus.FatalLevel: expterrlog,
		logrus.PanicLevel: expterrlog,
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

// experimentCleanup manages the cleanup operations for an experiment, such as sending emails if necessary,
// and copying, removing or archiving the logs
// TODO:  maybe a unit test for this
func (expt *ExperimentSuccess) experimentCleanup(ctx context.Context) error {
	if e := ctx.Err(); e != nil {
		return e
	}

	dir, err := os.Getwd()
	if err != nil {
		return errors.New(`Could not get current working directory.  Aborting cleanup.  
					Please check working directory and manually clean up log files`)
	}

	expterrfilepath := path.Join(dir, fmt.Sprintf(exptErrFilenamef, expt.Name))

	// Cleanup that must occur no matter what
	defer func(path string) error {
		// No experiment error logfile
		if _, err = os.Stat(path); os.IsNotExist(err) {
			return nil
		}
		if err := os.Remove(path); err != nil {
			return fmt.Errorf("Could not remove experiment error log %s.  Please clean up manually", path)
		}
		return nil
	}(expterrfilepath)

	// Experiment failed
	if !expt.Success {
		// Try to send email, which also deletes expt file, returns error
		// var err error = nil // Dummy
		// err := sendEmail(expt.Name, exptlogfilepath, emailSlice)
		// err := errors.New("Dummy error for email") // Take this line out and replace it with
		if viper.GetBool("test") {
			return nil // Don't do anything - we're testing.
		}
		data, err := ioutil.ReadFile(expterrfilepath)
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
			newfilename := fmt.Sprintf("%s-%s", expterrfilepath, time.Now().Format(time.RFC3339))
			newpath := path.Join(dir, newfilename)

			if err := os.Rename(expterrfilepath, newpath); err != nil {
				return fmt.Errorf("Could not move file %s to %s.  The error was %v", expterrfilepath, newpath, err)
			}
			return fmt.Errorf("Could not send email for experiment %s.  Archived error file at %s. "+
				"The error was %v", expt.Name, newpath, err)
		}
	}
	return nil
}

// Worker is the main function that manages the processes involved in generating and copying VOMS proxies to
// an experiment's nodes.  It returns a channel on which it reports the status of that experiment's proxy push.
// genlog is the logrus.logger that gets passed in that's meant to capture the non-experiment specific messages
// that might be printed from this module
func Worker(ctx context.Context, exptname string, genLog *logrus.Logger) <-chan ExperimentSuccess {
	var exptLog *logrus.Entry
	c := make(chan ExperimentSuccess, 2)
	expt := ExperimentSuccess{exptname, true} // Initialize

	exptLog, err := exptLogInit(ctx, expt.Name)
	if err != nil { // We either have panicked or it's a context error.  If it's the latter, we really don't care here
		exptLog = genLog.WithField("experiment", exptname)
	}

	exptLog.Debug("Now processing ", expt.Name)

	go func() {
		defer close(c) // All expt operations are done (either successful including cleanup or at error)
		exptConfig := viper.Sub("experiments." + expt.Name)
		successfulCopies := make(map[string][]string)
		failedCopies := make(map[string]map[string]struct{})
		badNodesSlice := make([]string, 0, len(exptConfig.GetStringSlice("nodes")))

		// Helper functions
		declareExptFailure := func() {
			expt.Success = false
			c <- expt
		}

		// Set up failedCopies for troubleshooting issues.  As pushes succeed, we'll be deleting these
		// from the map.
		for _, role := range exptConfig.GetStringMapString("accounts") {
			failedCopies[role] = make(map[string]struct{})
			for _, n := range exptConfig.GetStringSlice("nodes") {
				failedCopies[role][n] = struct{}{}
			}
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
		configNodes := make([]pingNoder, 0, len(exptConfig.GetStringSlice("nodes")))
		for _, n := range exptConfig.GetStringSlice("nodes") {
			configNodes = append(configNodes, node(n))
		}
		pingChannel := pingAllNodes(pingCtx, configNodes...)
		// Listen until we either timeout or the pingChannel is closed
	pingLoop:
		for {
			select {
			case <-pingCtx.Done():
				if e := pingCtx.Err(); e == context.DeadlineExceeded {
					exptLog.Errorf("Hit the ping timeout: %s", e)

				} else {
					exptLog.Error(e)
				}
				pingCancel()
				break pingLoop
			case testnode, chanOpen := <-pingChannel: // Receive on pingChannel
				if !chanOpen { // Break out of loop and proceed only if channel is not open
					pingCancel()
					break pingLoop
				}
				if testnode.err != nil {
					if n, ok := testnode.pingNoder.(node); ok {
						badNodesSlice = append(badNodesSlice, string(n))
					} else {
						exptLog.Errorf("Could not coerce interface pingNoder value %v to type node", testnode.pingNoder)
					}
					exptLog.Error(testnode.err)
				}
			}
		}

		if len(badNodesSlice) > 0 {
			exptLog.Errorf("The node(s) %s didn't return a response to ping after 5 "+
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
		vomsProxyObjects := make([]getProxyer, len(exptConfig.GetStringMapString("accounts")))
		vomsProxyObjects = createVomsProxyObjects(vpiCtx, exptConfig, viper.GetStringMapString("global"), expt.Name)
		vpiChan := getProxies(vpiCtx, vomsProxyObjects...)
		// Listen until we either timeout or vpiChan is closed
	vpiLoop:
		for {
			select {
			case <-vpiCtx.Done():
				if e := vpiCtx.Err(); e == context.DeadlineExceeded {
					exptLog.Errorf("Hit the voms-proxy-init timeout for %s: %s.  Check log for details. "+
						"Continuing to next proxy.\n", expt.Name, e)
				} else {
					exptLog.Error(e)
				}
				vpiCancel()
				break vpiLoop
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
			}
		}

		t, ok = viper.Get("copyTimeoutDuration").(time.Duration)
		if !ok {
			genLog.Error("copyTimeoutDuration is not a time.Duration object")
			declareExptFailure()
			return
		}
		copyCtx, copyCancel := context.WithTimeout(ctx, t)
		proxyTransferInfoObjects := make([]pushProxyer, len(exptConfig.GetStringSlice("nodes"))*len(exptConfig.GetStringMapString("accounts")))
		proxyTransferInfoObjects = createProxyTransferInfoObjects(copyCtx, exptConfig, badNodesSlice)
		copyChan := copyProxies(copyCtx, proxyTransferInfoObjects...)
		// if expt.Name == "darkside" {
		// 	time.Sleep(time.Duration(25) * time.Second)
		// }
		// Listen until we either timeout or the copyChan is closed
	copyLoop:
		for {
			select {
			case <-copyCtx.Done():
				if e := copyCtx.Err(); e == context.DeadlineExceeded {
					exptLog.Error("Experiment hit the timeout when waiting to push proxy to one of the nodes. " +
						"Please check the logs for details")
				} else {
					exptLog.Error(e)
				}
				expt.Success = false
				copyCancel()
				break copyLoop
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
					delete(failedCopies[pushproxy.role], pushproxy.node)
				}
			}
		}

		for role, nodes := range successfulCopies {
			sort.Strings(nodes)
			exptLog.Debugf("Successful copies for role %s were %v", role, nodes)
		}

		for role, nodes := range failedCopies {
			var nodesSlice []string
			if len(nodes) == 0 {
				continue
			}
			for n := range nodes {
				nodesSlice = append(nodesSlice, n)
			}
			sort.Strings(nodesSlice)
			exptLog.Errorf("Failed copies for role %s were %v", role, nodesSlice)
		}

		exptLog.Info("Finished processing ", expt.Name)
		c <- expt

		// We're logging the cleanup in the general log so that we don't create an extraneous
		// experiment log file
		exptLog.Info("Cleaning up ", expt.Name)
		if err := expt.experimentCleanup(ctx); err != nil {
			genLog.Errorf("Error cleaning up %s: %s", expt.Name, err)
		} else {
			genLog.Infof("Finished cleaning up %s with no errors", expt.Name)
		}
	}()
	return c
}
