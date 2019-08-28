// Package experiment contains all of the operations needed to push a VOMS X509 proxy as a part of the USDC Managed Proxy service that are
// experiment-specific.
package experiment

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"

	"cdcvs.fnal.gov/discompsupp/ken_proxy_push/v3/node"
	"cdcvs.fnal.gov/discompsupp/ken_proxy_push/v3/notifications"
	"cdcvs.fnal.gov/discompsupp/ken_proxy_push/v3/proxy"
)

var kinitExecutable = "/usr/krb5/bin/kinit"

// Success stores information on whether all the processes involved in generating, copying, and changing
// permissions on all proxies for an experiment were successful.
type Success struct {
	Name       string
	Successful bool
}

type (
	// TimeoutsConfig is a map of the timeouts passed in from the config file
	TimeoutsConfig map[string]time.Duration
	// KerbConfig contains information needed to run kinit
	KerbConfig map[string]string
)

// ExptConfig is a mega struct containing all the information the Worker needs to have or pass onto lower level funcs.
type ExptConfig struct {
	Name        string
	CertBaseDir string
	DestDir     string
	Nodes       []string
	Accounts    map[string]string
	VomsPrefix  string
	CertFile    string
	KeyFile     string
	IsTest      bool
	TimeoutsConfig
	KerbConfig
}

// Experiment worker-specific functions

// Worker is the main function that manages the processes involved in generating and copying VOMS proxies to
// an experiment's nodes.  It returns a channel on which it reports the status of that experiment's proxy push.
func Worker(ctx context.Context, eConfig ExptConfig, b notifications.BasicPromPush, nMgr notifications.Manager) <-chan Success {
	c := make(chan Success, 2)
	expt := Success{eConfig.Name, true} // Initialize
	genericTimeoutError := errors.New(genericTimeoutErrorString)

	log.WithField("experiment", eConfig.Name).Debug("Now processing experiment to push proxies")

	go func() {
		defer close(nMgr) // Send notifications
		defer close(c)    // All expt operations are done (either successful including cleanup or at error)

		// Helper functions
		declareExptFailure := func() {
			expt.Successful = false
			c <- expt
		}

		// General Setup
		successfulCopies := make(map[string][]string)
		failedCopies := make(map[string]map[string]error)
		badNodesSlice := make([]string, 0, len(eConfig.Nodes))

		// Set up failedCopies for troubleshooting issues.  As pushes succeed, we'll be deleting these
		// from the map.
		for _, role := range eConfig.Accounts {
			failedCopies[role] = make(map[string]error)
			for _, n := range eConfig.Nodes {
				failedCopies[role][n] = genericTimeoutError
			}
		}

		if _, ok := eConfig.KerbConfig["krb5ccname"]; !ok {
			krb5ConfigError := "Could not obtain KRB5CCNAME environmental variable from config. Please check the config file on fifeutilgpvm01."
			log.WithFields(log.Fields{
				"caller":     "experiment.Worker",
				"experiment": eConfig.Name,
			}).Error(krb5ConfigError)
			nMgr <- notifications.Notification{
				Message:          krb5ConfigError,
				Experiment:       eConfig.Name,
				NotificationType: notifications.SetupError,
			}
			declareExptFailure()
			return
		}

		// Get kerberos ticket
		// If we can't get a kerb ticket, log error and keep going.
		// We might have an old one that's still valid.
		if err := getKerbTicket(ctx, eConfig.KerbConfig); err != nil {
			var reportErrString string
			if e := ctx.Err(); e == context.DeadlineExceeded {
				reportErrString = genericTimeoutErrorString
				nMgr <- notifications.Notification{
					Message:          reportErrString,
					Experiment:       eConfig.Name,
					NotificationType: notifications.SetupError,
				}
				declareExptFailure()
				return
			}
			reportErrString = "Could not obtain new kerberos ticket.  Will try to use old one and push proxies."
			log.WithFields(log.Fields{
				"caller":     "experiment.Worker",
				"experiment": eConfig.Name,
			}).Warn(reportErrString)
			nMgr <- notifications.Notification{
				Message:          reportErrString,
				Experiment:       eConfig.Name,
				NotificationType: notifications.SetupError,
			}
		}

		// If check of exptConfig keys fails, experiment fails immediately
		if err := checkKeys(ctx, eConfig); err != nil {
			var reportErrString string
			if e := ctx.Err(); e == context.DeadlineExceeded {
				reportErrString = genericTimeoutErrorString
			} else {
				reportErrString = checkKeysErrorString
			}
			log.WithFields(log.Fields{
				"caller":     "experiment.Worker",
				"experiment": eConfig.Name,
			}).Error("Error processing experiment")
			declareExptFailure()
			nMgr <- notifications.Notification{
				Message:          reportErrString,
				Experiment:       eConfig.Name,
				NotificationType: notifications.SetupError,
			}
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
					pingTout := "Hit the timeout pinging nodes"
					log.WithField("experiment", eConfig.Name).Error(pingTout)
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
					n := testnode.PingNoder.String()
					badNodesSlice = append(badNodesSlice, n)
					log.WithFields(log.Fields{
						"experiment": eConfig.Name,
						"node":       n,
					}).Error(testnode.Err)
				}
			}
		}

		if len(badNodesSlice) > 0 {
			pingNodeAggMessagef := "The node(s) %s didn't return a response to ping after 5 " +
				"seconds.  Please investigate, and see if the nodes are up. " +
				"We'll still try to copy proxies there."
			nMsg := fmt.Sprintf(pingNodeAggMessagef, strings.Join(badNodesSlice, ", "))
			log.WithField("experiment", eConfig.Name).Error(nMsg)
			nMgr <- notifications.Notification{
				Message:          nMsg,
				Experiment:       eConfig.Name,
				NotificationType: notifications.SetupError,
			}
		}

		// Ingest service certs
		certs, err := getVomsProxyersForExperiment(ctx, eConfig.CertBaseDir, eConfig.CertFile, eConfig.KeyFile, eConfig.Accounts)
		if err != nil {
			msg := "Error setting up experiment:  one or more service certs could not be ingested"
			log.WithField("experiment", eConfig.Name).Error()
			expt.Successful = false
			nMgr <- notifications.Notification{
				Message:          msg,
				Experiment:       eConfig.Name,
				NotificationType: notifications.SetupError,
			}
		} else {
			log.WithField("experiment", eConfig.Name).Debug("Ingested service certs successfully")
		}

		// voms-proxy-init
		// If voms-proxy-init fails, we'll just continue on.  We'll still try to push proxies,
		// since they're valid for 24 hours
		vomsProxyStatuses := make([]vomsProxyInitStatus, 0, len(eConfig.Accounts))
		vpiCtx, vpiCancel := context.WithTimeout(ctx, eConfig.TimeoutsConfig["vpitimeoutDuration"])
		vpiChan := getVomsProxiesForExperiment(vpiCtx, certs, eConfig.VomsPrefix)

		// Listen until we either timeout or vpiChan is closed
	vpiLoop:
		for {
			select {
			case <-vpiCtx.Done():
				if e := vpiCtx.Err(); e == context.DeadlineExceeded {
					vpiTout := "Timeout obtaining VOMS proxies"
					log.WithFields(log.Fields{
						"caller":     "experimentutil.Worker",
						"experiment": eConfig.Name,
						"action":     "voms-proxy-init",
					}).Error(vpiTout)
				} else {
					log.WithFields(log.Fields{
						"caller":     "experimentutil.Worker",
						"experiment": eConfig.Name,
						"action":     "voms-proxy-init",
					}).Error(e)
				}
				vpiCancel()
				break vpiLoop

			case vpi, chanOpen := <-vpiChan: // receive on vpiChan
				if !chanOpen {
					vpiCancel()
					break vpiLoop
				}

				vomsProxyStatuses = append(vomsProxyStatuses, vpi)
				if vpi.err != nil {
					expt.Successful = false
					log.WithFields(log.Fields{
						"experiment": eConfig.Name,
						"role":       vpi.vomsProxy.Role,
					}).Error("Failed to generate voms proxy")
				} else {
					log.WithFields(log.Fields{
						"experiment":        eConfig.Name,
						"vomsProxyFilename": vpi.vomsProxy.Path,
					}).Debug("Generated voms proxy")
				}
			}
		}

		// Remove VOMS proxies that have been generated after Worker returns, or if there's a panic
		defer func() {
			r := recover()
			if r != nil {
				msg := fmt.Sprintf("Recovered from panic:  %s.  Will clean up voms proxies", r)
				log.WithField("experiment", eConfig.Name).Error(msg)
			}

			for _, v := range vomsProxyStatuses {
				if v.vomsProxy.Path == "" {
					continue
				}
				if err := v.vomsProxy.Remove(); err != nil {
					if !os.IsNotExist(err) {
						nMsg := "Failed to clean up experiment: could not delete VOMS proxy."
						log.WithFields(log.Fields{
							"experiment": eConfig.Name,
							"role":       v.vomsProxy.Role,
						}).Error(nMsg)
						nMgr <- notifications.Notification{
							Message:          nMsg,
							Experiment:       eConfig.Name,
							NotificationType: notifications.SetupError,
						}
					} else {
						log.WithFields(log.Fields{
							"experiment": eConfig.Name,
							"role":       v.vomsProxy.Role,
						}).Debug("Attempted to clean up VOMS proxy file, but file did not exist. Moving to next file.")
					}
				} else {
					log.WithFields(log.Fields{
						"experiment": eConfig.Name,
						"role":       v.vomsProxy.Role,
					}).Debug("Cleaned up VOMS Proxy")
				}
			}
		}()

		// We stop here in test mode.  Communicate success/failure and return
		if eConfig.IsTest {
			defer func() {
				log.WithField("experiment", eConfig.Name).Debug("In test mode - will not try to push proxies.  Returning from experiment.Worker now")
			}()

			// Failure to ping constitutes a failure in test mode
			if len(badNodesSlice) > 0 {
				declareExptFailure()
				return
			}
			c <- expt
			return
		}

		vomsProxies := make([]*proxy.VomsProxy, 0, len(eConfig.Accounts))

		for _, vpistatus := range vomsProxyStatuses {
			if vpistatus.err == nil {
				vomsProxies = append(vomsProxies, vpistatus.vomsProxy)
			} else {
				vpRole := vpistatus.vomsProxy.Role
				// We won't try to push proxies for this role.  Set all nodes' errors in table to be VPI error
				for node := range failedCopies[vpRole] {
					failedCopies[vpRole][node] = errors.New(genericVpiErrorString)
				}
			}

		}

		// Proxy transfer
		copyCfgs := createCopyFileConfigs(vomsProxies, eConfig.Accounts, eConfig.Nodes, eConfig.DestDir)

		copyCtx, copyCancel := context.WithTimeout(ctx, eConfig.TimeoutsConfig["copytimeoutDuration"])
		copyChan := copyAllProxies(copyCtx, copyCfgs)

		// Listen until we either timeout or the copyChan is closed
	copyLoop:
		for {
			select {
			case <-copyCtx.Done():
				if e := copyCtx.Err(); e == context.DeadlineExceeded {
					copyTout := "Timeout copying proxies to destination"
					log.WithFields(log.Fields{
						"caller":     "experiment.Worker",
						"experiment": eConfig.Name,
						"action":     "copy proxies",
					}).Error(copyTout)
				} else {
					log.WithFields(log.Fields{
						"caller":     "experiment.Worker",
						"experiment": eConfig.Name,
						"action":     "copy proxies",
					}).Error(e)
					msg := []string{generalContextErrorString}
					for role, nodeMap := range failedCopies {
						for node, err := range nodeMap {
							failedCopies[role][node] = generateNewErrorStringForTable(
								genericTimeoutError,
								err,
								msg,
								"; ",
							)
						}
					}
				}

				expt.Successful = false
				copyCancel()
				break copyLoop

			case pushproxy, chanOpen := <-copyChan:
				if !chanOpen {
					copyCancel()
					break copyLoop
				}

				log.WithField("experiment", eConfig.Name).Debug(pushproxy)
				if pushproxy.err != nil {
					var copyProxyErrorSlice []string
					copyProxyErrorSlice = []string{"Error copying proxy"}

					for _, n := range badNodesSlice {
						if pushproxy.node == n {
							//copyProxyErrorf = copyProxyErrorf + " The node was not pingable earlier, so please look into the status of that node to make sure it's up and working."
							copyProxyErrorSlice = append(copyProxyErrorSlice, "Node not pingable earlier")
							break
						}
					}

					log.WithFields(log.Fields{
						"caller":  "experiment.Worker",
						"account": pushproxy.account,
						"node":    pushproxy.node,
						"role":    pushproxy.role,
						"action":  "copy proxies",
					}).Error(pushproxy.err)

					failedCopies[pushproxy.role][pushproxy.node] = generateNewErrorStringForTable(
						genericTimeoutError,
						failedCopies[pushproxy.role][pushproxy.node],
						copyProxyErrorSlice,
						"; ",
					)

					expt.Successful = false
				} else {
					successfulCopies[pushproxy.role] = append(successfulCopies[pushproxy.role], pushproxy.node)
					delete(failedCopies[pushproxy.role], pushproxy.node)
					if err := b.PushNodeRoleTimestamp(expt.Name, pushproxy.node, pushproxy.role); err != nil {
						msg := "Could not report success metrics to prometheus"
						log.WithFields(log.Fields{
							"caller": "experiment.Worker",
							"action": "prometheus metric push",
							"node":   pushproxy.node,
							"role":   pushproxy.role,
						}).Warn(msg)
					} else {
						log.WithFields(log.Fields{
							"node": pushproxy.node,
							"role": pushproxy.role,
						}).Debug("Pushed prometheus success timestamp")
					}
				}
			}
		}

		// Note successes and failures
		for role, nodes := range successfulCopies {
			sort.Strings(nodes)
			log.WithFields(log.Fields{
				"role":  role,
				"nodes": strings.Join(nodes, ", "),
			}).Debugf("Successful copies")
		}

		failedMsg := failedPrettifyRolesNodesMap(failedCopies)
		if len(failedMsg) > 0 {
			nMgr <- notifications.Notification{
				Message:          failedMsg,
				Experiment:       eConfig.Name,
				NotificationType: notifications.RunError,
			}
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
			}).Errorf("Failed copies")
		}

		log.WithField("experiment", eConfig.Name).Info("Finished processing experiment")
		// Put experiment success status into channel now so that it can be evaulated concurrently with cleanup
		c <- expt

		// Cleanup is handled with deferred funcs
		log.WithField("experiment", eConfig.Name).Info("Cleaning up experiment")

	}()

	return c
}

// Notifications messages
const (
	generalContextErrorString = "Context error for experiment"
	checkKeysErrorString      = `Input file improperly formatted (accounts or nodes don't 
			exist for this experiment). Please check the config file on fifeutilgpvm01.
			 I will skip this experiment for now`
	genericTimeoutErrorString = "Timeout error"
	genericVpiErrorString     = "Failed to generate VOMS proxy"
)
