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

	"cdcvs.fnal.gov/discompsupp/ken_proxy_push/notifications"
	"github.com/rifflock/lfshook"
	"github.com/sirupsen/logrus"
)

const exptErrFilenamef string = "golang_proxy_push_%s.log" // temp file per experiment that will be emailed to experiment

var genLog *logrus.Logger
var rwmuxErr, rwmuxLog, rwmuxDebug sync.RWMutex // mutexes to be used when copying experiment logs into master and error log

// ExperimentSuccess stores information on whether all the processes involved in generating, copying, and changing
// permissions on all proxies for an experiment were successful.
type ExperimentSuccess struct {
	Name    string
	Success bool
}

// ExptErrorFormatter is a struct that implements the logrus.Formatter interface.  It's for experiment logs that will be emailed to experiments in
// case of error
type ExptErrorFormatter struct {
}

type (
	// TimeoutsConfig is a map of the timeouts passed in from the config file
	TimeoutsConfig map[string]time.Duration
	// LogsConfig is a map of the location of the logfiles
	LogsConfig map[string]string
	// VPIConfig contains information needed to run the voms-proxy-init command
	VPIConfig map[string]string
	// KerbConfig contains information needed to run kinit
	KerbConfig map[string]string
	// PingConfig contains information needed to ping the various interactive nodes
	PingConfig map[string]string
	// SSHConfig contains the common options and arguments necessary to run scp and ssh; chmod to copy the proxies into place
	SSHConfig map[string]string
)

// ExptConfig is a mega struct containing all the information the Worker needs to have or pass onto lower level funcs.
type ExptConfig struct {
	Name        string
	CertBaseDir string
	Krb5ccname  string
	DestDir     string
	ExptEmails  []string
	Nodes       []string
	Accounts    map[string]string
	VomsPrefix  string
	CertFile    string
	KeyFile     string
	IsTest      bool
	NConfig     notifications.Config
	TimeoutsConfig
	LogsConfig
	VPIConfig
	KerbConfig
	PingConfig
	SSHConfig
}

// Experiment worker-specific functions

// Setup and cleanup

// setupNotificationsConfig sets the values for the notifications.config sub-type of ExptConfig to the appropriate values for the experiment
func setupNotificationsConfig(pExptConfig *ExptConfig) {
	pExptConfig.NConfig.From = pExptConfig.NConfig.ConfigInfo["admin_email"]
	if !pExptConfig.IsTest {
		pExptConfig.NConfig.To = pExptConfig.ExptEmails
	}
	pExptConfig.NConfig.Subject = "Managed Proxy Push errors for " + pExptConfig.Name
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

// exptLogInit sets up the logrus instance for the experiment worker
// It returns a pointer to a logrus.Entry object that can be used to log events.
func exptLogInit(ctx context.Context, ename string, lConfig LogsConfig) (*logrus.Entry, error) {
	if e := ctx.Err(); e != nil {
		return nil, e
	}

	var Log = logrus.New()
	expterrlog := fmt.Sprintf(exptErrFilenamef, ename)

	Log.SetLevel(logrus.DebugLevel)

	Log.AddHook(lfshook.NewHook(lfshook.PathMap{
		logrus.DebugLevel: lConfig["debugfile"],
		logrus.InfoLevel:  lConfig["debugfile"],
		logrus.WarnLevel:  lConfig["debugfile"],
		logrus.ErrorLevel: lConfig["debugfile"],
		logrus.FatalLevel: lConfig["debugfile"],
		logrus.PanicLevel: lConfig["debugfile"],
	}, &logrus.TextFormatter{FullTimestamp: true}))

	Log.AddHook(lfshook.NewHook(lfshook.PathMap{
		logrus.InfoLevel:  lConfig["logfile"],
		logrus.WarnLevel:  lConfig["logfile"],
		logrus.ErrorLevel: lConfig["logfile"],
		logrus.FatalLevel: lConfig["logfile"],
		logrus.PanicLevel: lConfig["logfile"],
	}, &logrus.TextFormatter{FullTimestamp: true}))

	// General Error Log that will get sent to Admins
	Log.AddHook(lfshook.NewHook(lfshook.PathMap{
		logrus.ErrorLevel: lConfig["errfile"],
		logrus.FatalLevel: lConfig["errfile"],
		logrus.PanicLevel: lConfig["errfile"],
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
func getKerbTicket(ctx context.Context, krbConfig KerbConfig) error {
	os.Setenv("KRB5CCNAME", krbConfig["krb5ccname"])
	kerbcmdargs := strings.Fields(krbConfig["kinitargs"])

	cmd := exec.CommandContext(ctx, krbConfig["kinitexecutable"], kerbcmdargs...)
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
func checkKeys(ctx context.Context, eConfig ExptConfig) error {
	if e := ctx.Err(); e != nil {
		return e
	}

	if len(eConfig.Nodes) == 0 || len(eConfig.Accounts) == 0 {
		return errors.New(`Input file improperly formatted (accounts or nodes don't 
			exist for this experiment). Please check the config file on fifeutilgpvm01.
			 I will skip this experiment for now`)
	}
	return nil
}

// experimentCleanup manages the cleanup operations for an experiment, such as sending emails if necessary,
// and copying, removing or archiving the logs
// TODO:  maybe a unit test for this
func (expt *ExperimentSuccess) experimentCleanup(ctx context.Context, exptConfig ExptConfig) error {
	if e := ctx.Err(); e != nil {
		return e
	}

	dir, err := os.Getwd()
	if err != nil {
		msg := `Could not get current working directory.  Aborting cleanup.  
					Please check working directory and manually clean up log files`
		genLog.WithFields(logrus.Fields{"experiment": exptConfig.Name, "caller": "experimentCleanup"}).Error(msg)
		return errors.New(msg)
	}

	expterrfilepath := path.Join(dir, fmt.Sprintf(exptErrFilenamef, expt.Name))

	// Cleanup that must occur no matter what
	defer func(path string) error {
		// No experiment error logfile
		if _, err = os.Stat(path); os.IsNotExist(err) {
			return nil
		} else {
			genLog.WithFields(logrus.Fields{"experiment": exptConfig.Name, "caller": "experimentCleanup"}).Error(err)
		}
		if err := os.Remove(path); err != nil {
			genLog.WithFields(logrus.Fields{"experiment": exptConfig.Name, "caller": "experimentCleanup"}).Error(err)
			return fmt.Errorf("Could not remove experiment error log %s.  Please clean up manually", path)
		}
		return nil
	}(expterrfilepath)

	// Experiment failed
	if !expt.Success {
		// Try to send email, which also deletes expt file, returns error
		if exptConfig.IsTest {
			return nil // Don't do anything - we're testing.
		}
		data, err := ioutil.ReadFile(expterrfilepath)
		if err != nil {
			genLog.WithFields(logrus.Fields{"experiment": exptConfig.Name, "caller": "experimentCleanup"}).Error(err)
			return err
		}

		msg := string(data)

		emailCtx, emailCancel := context.WithTimeout(ctx, exptConfig.TimeoutsConfig["emailtimeoutDuration"])
		defer emailCancel()
		if err := notifications.SendEmail(emailCtx, exptConfig.NConfig, msg); err != nil {
			genLog.WithFields(logrus.Fields{"experiment": exptConfig.Name, "caller": "experimentCleanup"}).Error(err)
			msg := "Error sending email.  Will archive error file"
			genLog.WithFields(logrus.Fields{"experiment": exptConfig.Name, "caller": "experimentCleanup"}).Error(msg)
			newfilename := fmt.Sprintf("%s-%s", expterrfilepath, time.Now().Format(time.RFC3339))
			newpath := path.Join(dir, newfilename)

			if err := os.Rename(expterrfilepath, newpath); err != nil {
				genLog.WithFields(logrus.Fields{"experiment": exptConfig.Name, "caller": "experimentCleanup"}).Error(err)
				return fmt.Errorf("Could not move file %s to %s.", expterrfilepath, newpath)
			}
			return fmt.Errorf("Could not send email for experiment %s.  Archived error file at %s.", expt.Name, newpath)
		}
	}
	return nil
}

// Worker is the main function that manages the processes involved in generating and copying VOMS proxies to
// an experiment's nodes.  It returns a channel on which it reports the status of that experiment's proxy push.
// genlog is the logrus.logger that gets passed in that's meant to capture the non-experiment specific messages
// that might be printed from this module
func Worker(ctx context.Context, eConfig ExptConfig, genLog *logrus.Logger, b notifications.BasicPromPush) <-chan ExperimentSuccess {
	var exptLog *logrus.Entry
	c := make(chan ExperimentSuccess, 2)
	expt := ExperimentSuccess{eConfig.Name, true} // Initialize

	exptLog, err := exptLogInit(ctx, eConfig.Name, eConfig.LogsConfig)
	if err != nil { // We either have panicked or it's a context error.  If it's the latter, we really don't care here
		msg := "Error setting up experiment log.  Will log in general log"
		genLog.WithFields(logrus.Fields{"experiment": eConfig.Name}).Error(msg)
		exptLog = genLog.WithField("experiment", eConfig.Name)
	}

	exptLog.Debug("Now processing ", eConfig.Name)

	go func() {
		defer close(c) // All expt operations are done (either successful including cleanup or at error)

		// Helper functions
		declareExptFailure := func() {
			expt.Success = false
			c <- expt
		}

		// General Setup
		setupNotificationsConfig(&eConfig)
		successfulCopies := make(map[string][]string)
		failedCopies := make(map[string]map[string]struct{})
		badNodesSlice := make([]string, 0, len(eConfig.Nodes))

		// Set up failedCopies for troubleshooting issues.  As pushes succeed, we'll be deleting these
		// from the map.
		for _, role := range eConfig.Accounts {
			failedCopies[role] = make(map[string]struct{})
			for _, n := range eConfig.Nodes {
				failedCopies[role][n] = struct{}{}
			}
		}

		if _, ok := eConfig.KerbConfig["krb5ccname"]; !ok {
			exptLog.Error("Could not obtain KRB5CCNAME environmental variable from config. " +
				"Please check the config file on fifeutilgpvm01.")
			declareExptFailure()
			return
		}
		// If we can't get a kerb ticket, log error and keep going.
		// We might have an old one that's still valid.
		if err := getKerbTicket(ctx, eConfig.KerbConfig); err != nil {
			exptLog.Warn(err)
		}

		// If check of exptConfig keys fails, experiment fails immediately
		if err := checkKeys(ctx, eConfig); err != nil {
			exptLog.WithFields(logrus.Fields{"experiment": eConfig.Name}).Error(err)
			declareExptFailure()
			return
		}
		exptLog.Debug("Config keys are valid")

		// Ping nodes to make sure they're up

		pingCtx, pingCancel := context.WithTimeout(ctx, eConfig.TimeoutsConfig["pingtimeoutDuration"])
		configNodes := make([]pingNoder, 0, len(eConfig.Nodes))
		for _, n := range eConfig.Nodes {
			configNodes = append(configNodes, node(n))
		}
		pingChannel := pingAllNodes(pingCtx, eConfig.PingConfig, configNodes...)
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

		// voms-proxy-init
		// If voms-proxy-init fails, we'll just continue on.  We'll still try to push proxies,
		// since they're valid for 24 hours
		vpiCtx, vpiCancel := context.WithTimeout(ctx, eConfig.TimeoutsConfig["vpitimeoutDuration"])
		vomsProxyObjects := createVomsProxyObjects(vpiCtx, eConfig)
		vpiChan := getProxies(vpiCtx, eConfig.VPIConfig, vomsProxyObjects...)
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

		// Proxy transfer
		copyCtx, copyCancel := context.WithTimeout(ctx, eConfig.TimeoutsConfig["copytimeoutDuration"])
		proxyTransferInfoObjects := createProxyTransferInfoObjects(copyCtx, eConfig, badNodesSlice)
		copyChan := copyProxies(copyCtx, eConfig.SSHConfig, proxyTransferInfoObjects...)

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
					if err := b.PushNodeRoleTimestamp(expt.Name, pushproxy.node, pushproxy.role); err != nil {
						genLog.WithField("experiment", expt.Name).Warnf(
							"Could not report success metrics to prometheus for [node, role] = [%s, %s]", pushproxy.node, pushproxy.role)
					} else {
						exptLog.WithField("experiment", expt.Name).Debugf(
							"Pushed prometheus success timestamp for [node, role] = [%s, %s]", pushproxy.node, pushproxy.role)
					}
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
		if err := expt.experimentCleanup(ctx, eConfig); err != nil {
			genLog.WithField("experiment", expt.Name).Error("Error cleaning up experiment")
		} else {
			genLog.WithField("experiment", expt.Name).Info("Finished cleaning up with no errors")
		}
	}()
	return c
}
