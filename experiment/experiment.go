// Package experimentutil contains all of the operations needed to push a VOMS X509 proxy as a part of the USDC Managed Proxy service that are
// experiment-specific.
package experimentutil

import (
	"context"
	"sort"
	"strings"
	"sync"
	"time"

	"cdcvs.fnal.gov/discompsupp/ken_proxy_push/node"
	"cdcvs.fnal.gov/discompsupp/ken_proxy_push/notifications"
	"cdcvs.fnal.gov/discompsupp/ken_proxy_push/proxy"

	"cdcvs.fnal.gov/discompsupp/ken_proxy_push/proxyPushLogger"
	log "github.com/sirupsen/logrus"
)

// const exptErrFilenamef string = "golang_proxy_push_%s.log" // temp file per experiment that will be emailed to experiment

// var genLog *logrus.Logger

var (
	rwmuxErr, rwmuxLog, rwmuxDebug sync.RWMutex // mutexes to be used when copying experiment logs into master and error log
	kinitExecutable                = "/usr/krb5/bin/kinit"
)

// ExperimentSuccess stores information on whether all the processes involved in generating, copying, and changing
// permissions on all proxies for an experiment were successful.
type ExperimentSuccess struct {
	Name    string
	Success bool
}

type (
	// TimeoutsConfig is a map of the timeouts passed in from the config file
	TimeoutsConfig map[string]time.Duration
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
	Nodes       []string
	Accounts    map[string]string
	VomsPrefix  string
	CertFile    string
	KeyFile     string
	IsTest      bool
	NConfig     notifications.Config
	TimeoutsConfig
	LogsConfig proxyPushLogger.LogsConfig
	VPIConfig
	KerbConfig
	PingConfig
	SSHConfig
}

// Experiment worker-specific functions

// Worker is the main function that manages the processes involved in generating and copying VOMS proxies to
// an experiment's nodes.  It returns a channel on which it reports the status of that experiment's proxy push.
func Worker(ctx context.Context, eConfig ExptConfig, b notifications.BasicPromPush) <-chan ExperimentSuccess {
	c := make(chan ExperimentSuccess, 2)
	expt := ExperimentSuccess{eConfig.Name, true} // Initialize

	log.WithField("experiment", eConfig.Name).Debug("Now processing experiment to push proxies")

	go func() {
		defer close(c) // All expt operations are done (either successful including cleanup or at error)

		// Helper functions
		declareExptFailure := func() {
			expt.Success = false
			c <- expt
		}

		// General Setup
		//setupNotificationsConfig(&eConfig)
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
			log.WithFields(log.Fields{
				"caller":     "experiment.Worker",
				"experiment": eConfig.Name,
			}).Error("Could not obtain KRB5CCNAME environmental variable from config. " +
				"Please check the config file on fifeutilgpvm01.")
			declareExptFailure()
			return
		}
		// If we can't get a kerb ticket, log error and keep going.
		// We might have an old one that's still valid.
		if err := getKerbTicket(ctx, eConfig.KerbConfig); err != nil {
			log.WithFields(log.Fields{
				"caller":     "experiment.Worker",
				"experiment": eConfig.Name,
			}).Warn("Obtaining a kerberos ticket failed.  May be unable to push proxies")
		}

		// If check of exptConfig keys fails, experiment fails immediately
		if err := checkKeys(ctx, eConfig); err != nil {
			log.WithFields(log.Fields{
				"caller":     "experiment.Worker",
				"experiment": eConfig.Name,
			}).Error("Error processing experiment")
			declareExptFailure()
			return
		}
		log.WithField("experiment", eConfig.Name).Debug("Config keys are valid")

		// Ping nodes to make sure they're up

		pingCtx, pingCancel := context.WithTimeout(ctx, eConfig.TimeoutsConfig["pingtimeoutDuration"])
		configNodes := make([]node.PingNoder, 0, len(eConfig.Nodes))
		for _, n := range eConfig.Nodes {
			configNodes = append(configNodes, node.NewNode(n))
		}
		pingChannel := node.PingAllNodes(pingCtx, configNodes...)
		// Listen until we either timeout or the pingChannel is closed
	pingLoop:
		for {
			select {
			case <-pingCtx.Done():
				if e := pingCtx.Err(); e == context.DeadlineExceeded {
					log.WithField("experiment", eConfig.Name).Error("Hit the ping timeout")

				} else {
					log.WithField("experiment", eConfig.Name).Error(e)
				}
				pingCancel()
				break pingLoop
			case testnode, chanOpen := <-pingChannel: // Receive on pingChannel
				if !chanOpen { // Break out of loop and proceed only if channel is not open
					pingCancel()
					break pingLoop
				}
				if testnode.Err != nil {
					n := testnode.PingNoder.NodeAsString()
					badNodesSlice = append(badNodesSlice, n)
					log.WithFields(log.Fields{
						"experiment": eConfig.Name,
						"node":       n,
					}).Error(testnode.Err)
				}
			}
		}

		if len(badNodesSlice) > 0 {
			log.Errorf("The node(s) %s didn't return a response to ping after 5 "+
				"seconds.  Please investigate, and see if the nodes are up. "+
				"We'll still try to copy proxies there.", strings.Join(badNodesSlice, ", "))
		}

		// TODO  Add notification manager
		// Ingest service certs
		certs, err := getVomsProxyersForExperiment(ctx, eConfig.CertBaseDir, eConfig.CertFile, eConfig.KeyFile, eConfig.Accounts)
		if err != nil {
			log.WithField("experiment", eConfig.Name).Error("Error setting up experiment:  one or more service certs could not be ingested")
			expt.Success = false
		}

		// Create voms proxy from that

		// Push that proxy

		// voms-proxy-init
		// If voms-proxy-init fails, we'll just continue on.  We'll still try to push proxies,
		// since they're valid for 24 hours
		vomsProxies := make([]*proxy.VomsProxy, len(eConfig.Accounts))
		vpiCtx, vpiCancel := context.WithTimeout(ctx, eConfig.TimeoutsConfig["vpitimeoutDuration"])
		vpiChan := getVomsProxiesForExperiment(vpiCtx, certs, eConfig.VomsPrefix)
		// Listen until we either timeout or vpiChan is closed
	vpiLoop:
		for {
			select {
			case <-vpiCtx.Done():
				if e := vpiCtx.Err(); e == context.DeadlineExceeded {
					log.WithFields(log.Fields{
						"caller": "experimentutil.Worker",
						"action": "voms-proxy-init",
					}).Error("Timeout obtaining VOMS proxies")
				} else {
					log.WithFields(log.Fields{
						"caller": "experimentutil.Worker",
						"action": "voms-proxy-init",
					}).Error(e)
				}
				vpiCancel()
				break vpiLoop
			case vpi, chanOpen := <-vpiChan: // receive on vpiChan
				if !chanOpen {
					vpiCancel()
					break vpiLoop
				}
				if vpi.err != nil {
					expt.Success = false
				} else {
					log.WithField("vomsProxyFilename", vpi.vomsProxy.Path).Debug("Generated voms proxy")
					vomsProxies = append(vomsProxies, vpi.vomsProxy)
				}
			}
		}

		// Proxy transfer
		copyCfgs := createCopyFileConfigs(vomsProxies, eConfig.Accounts, eConfig.Nodes, eConfig.DestDir)

		copyCtx, copyCancel := context.WithTimeout(ctx, eConfig.TimeoutsConfig["copytimeoutDuration"])
		copyChan := copyAllProxies(copyCtx, copyCfgs)

		// TODO Where I left off
		// Listen until we either timeout or the copyChan is closed
	copyLoop:
		for {
			select {
			case <-copyCtx.Done():
				if e := copyCtx.Err(); e == context.DeadlineExceeded {
					log.WithFields(log.Fields{
						"caller": "experiment.Worker",
						"action": "copy proxies",
					}).Error("Hit timeout copying proxies")
				} else {
					log.WithFields(log.Fields{
						"caller": "experiment.Worker",
						"action": "copy proxies",
					}).Error(e)
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
					log.WithFields(log.Fields{
						"caller": "experiment.Worker",
						"action": "copy proxies",
					}).Error(pushproxy.err)
					expt.Success = false
				} else {
					successfulCopies[pushproxy.role] = append(successfulCopies[pushproxy.role], pushproxy.node)
					delete(failedCopies[pushproxy.role], pushproxy.node)
					if err := b.PushNodeRoleTimestamp(expt.Name, pushproxy.node, pushproxy.role); err != nil {
						log.WithFields(log.Fields{
							"caller": "experiment.Worker",
							"action": "prometheus metric push",
							"node":   pushproxy.node,
							"role":   pushproxy.role,
						}).Warn(
							"Could not report success metrics to prometheus")
					} else {
						log.WithFields(log.Fields{
							"node": pushproxy.node,
							"role": pushproxy.role,
						}).Debug("Pushed prometheus success timestamp")
					}
				}
			}
		}

		for role, nodes := range successfulCopies {
			sort.Strings(nodes)
			log.WithFields(log.Fields{
				"role":  role,
				"nodes": strings.Join(nodes, ", "),
			}).Debugf("Successful copies")
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
			nodesString := strings.Join(nodesSlice, ", ")
			log.WithFields(log.Fields{
				"role":  role,
				"nodes": nodesString,
			}).Errorf("Failed copies for role %s were %s", role, nodesString)
		}

		log.WithField("experiment", eConfig.Name).Info("Finished processing experiment")
		// Put experiment success status into channel now so that it can be evaulated concurrently with cleanup
		c <- expt

		// Cleanup
		log.WithField("experiment", eConfig.Name).Info("Cleaning up experiment")

		for _, v := range vomsProxies {
			if err := v.Remove(); err != nil {
				log.WithFields(log.Fields{
					"experiment": eConfig.Name,
					"role":       v.Role,
				}).Error("Failed to clean up experiment: could not delete VOMS proxy.  Please review log")
			} else {
				log.WithFields(log.Fields{
					"experiment": eConfig.Name,
					"role":       v.Role,
				}).Debug("Cleaned up VOMS Proxy")
			}
		}
		// Close notifications channel

		//		if err := expt.experimentCleanup(ctx, eConfig); err != nil {
		//			log.WithField("experiment", eConfig.Name).Error("Error cleaning up experiment")
		//		} else {
		//			log.WithField("experiment", eConfig.Name).Info("Finished cleaning up with no errors")
		//		}
	}()
	return c
}

// Setup and cleanup

// experimentCleanup manages the cleanup operations for an experiment, such as sending emails if necessary,
// and copying, removing or archiving the logs
//// TODO:  maybe a unit test for this
//func (expt *ExperimentSuccess) experimentCleanup(ctx context.Context, exptConfig ExptConfig) error {
//	if e := ctx.Err(); e != nil {
//		return e
//	}
//
//	dir, err := os.Getwd()
//	if err != nil {
//		msg := `Could not get current working directory.  Aborting cleanup.
//					Please check working directory and manually clean up log files`
//		exptConfig.Logger.Error(err)
//		return errors.New(msg)
//	}
//
//	expterrfilepath := proxyPushLogger.GetExptErrorLogfileName(expt.Name)
//
//	// Cleanup that must occur no matter what
//	defer func(path string) error {
//		// No experiment error logfile
//		if _, err = os.Stat(path); os.IsNotExist(err) {
//			return nil
//		} else if err != nil {
//			exptConfig.Logger.Error(err)
//		}
//		if err := os.Remove(path); err != nil {
//			exptConfig.Logger.Error(err)
//			msg := fmt.Errorf("Could not remove experiment error log %s.  Please clean up manually", path)
//			exptConfig.Logger.Error(msg)
//			return msg
//		}
//		exptConfig.Logger.WithField("filename", path).Debug("Removed experiment error file")
//		return nil
//	}(expterrfilepath)
//
//	// Experiment failed
//	if !expt.Success {
//		// Try to send email, which also deletes expt file, returns error
//		if exptConfig.IsTest {
//			return nil // Don't do anything - we're testing.
//		}
//		data, err := ioutil.ReadFile(expterrfilepath)
//		if err != nil {
//			exptConfig.Logger.Error(err)
//			return err
//		}
//
//		msg := string(data)
//
//		emailCtx, emailCancel := context.WithTimeout(ctx, exptConfig.TimeoutsConfig["emailtimeoutDuration"])
//		defer emailCancel()
//		if err := notifications.SendEmail(emailCtx, exptConfig.NConfig, msg); err != nil {
//			exptConfig.Logger.Error("Error cleaning up.  Will archive error file.")
//			newfilename := fmt.Sprintf("%s-%s", expterrfilepath, time.Now().Format(time.RFC3339))
//			newpath := path.Join(dir, newfilename)
//
//			if err := os.Rename(expterrfilepath, newpath); err != nil {
//				exptConfig.Logger.Error(err)
//				exptConfig.Logger.Errorf("Could not move file %s to %s.", expterrfilepath, newpath)
//				return err
//			}
//			return fmt.Errorf("could not send email for experiment %s.  Archived error file at %s", expt.Name, newpath)
//		}
//	}
//	return nil
//}
