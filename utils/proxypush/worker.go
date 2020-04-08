package proxypush

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	log "github.com/sirupsen/logrus"

	"cdcvs.fnal.gov/discompsupp/ken_proxy_push/v3/notifications"
	"cdcvs.fnal.gov/discompsupp/ken_proxy_push/v3/utils"
)

var kinitExecutable = "/usr/krb5/bin/kinit"

// Success stores information on whether all the processes involved in generating, copying, and changing
// permissions on all proxies for an experiment were successful.
type Success struct {
	Name       string
	Successful bool
}

// Experiment worker-specific functions

// worker is the main function that manages the processes involved in generating and copying VOMS proxies to
// an experiment's nodes.  It returns a channel on which it reports the status of that experiment's proxy push.
func worker(ctx context.Context, eConfig *utils.ExptConfig, b notifications.BasicPromPush, nMgr notifications.Manager) <-chan Success {
	c := make(chan Success, 2)
	expt := Success{eConfig.Name, true} // Initialize
	genericTimeoutError := errors.New(genericTimeoutErrorString)

	log.WithField("experiment", eConfig.Name).Debug("Now processing experiment to push proxies")

	teardownMultiple := func(funcs []func() error) error {
		var errExists bool
		c := make(chan error, len(funcs))
		for _, f := range funcs {
			func(fn func() error) {
				c <- fn()
			}(f)
		}
		close(c)

		for err := range c {
			if err != nil {
				errExists = true
			}
		}

		if errExists {
			msg := "One or more teardown functions failed.  Please look at the logs"
			log.WithField("experiment", eConfig.Name).Error(msg)
			return errors.New(msg)
		}
		return nil
	}

	go func() {
		defer close(nMgr) // Send notifications
		defer close(c)    // All expt operations are done (either successful including cleanup or at error)

		// Helper functions
		declareExptFailure := func() {
			expt.Successful = false
			c <- expt
		}

		// General Setup:
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
		configNodes := make([]PingNoder, 0, len(eConfig.Nodes))
		for _, n := range eConfig.Nodes {
			configNodes = append(configNodes, NewNode(n))
		}
		pingChannel := PingAllNodes(pingCtx, configNodes...)

		// Listen until we either timeout or the pingChannel is closed
		func() {
			defer pingCancel()
			for {
				select {
				case <-pingCtx.Done():
					if e := pingCtx.Err(); e == context.DeadlineExceeded {
						pingTout := "Hit the timeout pinging nodes"
						log.WithField("experiment", eConfig.Name).Error(pingTout)
					} else {
						log.WithField("experiment", eConfig.Name).Error(e)
					}
					return

				case testnode, chanOpen := <-pingChannel: // Receive on pingChannel
					if !chanOpen { // Break out of loop and proceed only if channel is not open
						return
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
		}()

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
		certMap := make(map[string]*certKeyPair)
		for account, role := range eConfig.Accounts {
			certMap[role] = getCertKeyPair(
				eConfig.CertBaseDir,
				eConfig.CertFile,
				eConfig.KeyFile,
				account,
			)
		}

		certs, err := ingestServiceCertsForExperiment(ctx, certMap)
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
		vpiChan := generateVomsProxiesForExperiment(vpiCtx, certs, eConfig.VomsPrefix)

		// Listen until we either timeout or vpiChan is closed
		func() {
			defer vpiCancel()
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
					return

				case vpi, chanOpen := <-vpiChan: // receive on vpiChan
					if !chanOpen {
						return
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
		}()

		// Remove VOMS proxies that have been generated after Worker returns, or if there's a panic
		defer func() {
			r := recover()
			if r != nil {
				msg := fmt.Sprintf("Recovered from panic:  %s.  Will clean up voms proxies", r)
				log.WithField("experiment", eConfig.Name).Error(msg)
			}
			teardownFuncs := make([]func() error, 0, len(vomsProxyStatuses))
			for _, vpiStatus := range vomsProxyStatuses {
				teardownFuncs = append(teardownFuncs, vpiStatus.teardownFunc)
			}
			teardownMultiple(teardownFuncs)
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

		vomsProxies := make([]*vomsProxy, 0, len(eConfig.Accounts))

		for _, vpiStatus := range vomsProxyStatuses {
			if vpiStatus.err == nil {
				vomsProxies = append(vomsProxies, vpiStatus.vomsProxy)
			} else {
				vpRole := vpiStatus.vomsProxy.Role
				// We won't try to push proxies for this role.  Set all nodes' errors in table to be VPI error
				for node := range failedCopies[vpRole] {
					failedCopies[vpRole][node] = &VomsProxyInitError{}
				}
			}

		}

		// Proxy transfer
		copyCfgs := createCopyFileConfigs(vomsProxies, eConfig.Accounts, eConfig.Nodes, eConfig.DestDir, eConfig.SSHOpts)
		copyCtx, copyCancel := context.WithTimeout(ctx, eConfig.TimeoutsConfig["copytimeoutDuration"])
		copyChan := copyAllProxies(copyCtx, copyCfgs)

		// Listen until we either timeout or the copyChan is closed
		func() {
			defer copyCancel()
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
								failedCopies[role][node] = utils.GenerateNewErrorStringForTable(
									genericTimeoutError,
									err,
									msg,
									"; ",
								)
							}
						}
					}

					expt.Successful = false
					return

				case pushproxy, chanOpen := <-copyChan:
					if !chanOpen {
						return
					}

					log.WithField("experiment", eConfig.Name).Debug(pushproxy)
					if pushproxy.err != nil {
						var copyProxyErrorSlice []string
						copyProxyErrorSlice = []string{"Error copying proxy"}

						for _, n := range badNodesSlice {
							if pushproxy.node == n {
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

						failedCopies[pushproxy.role][pushproxy.node] = utils.GenerateNewErrorStringForTable(
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
		}()

		// Note successes and failures
		for role, nodes := range successfulCopies {
			sort.Strings(nodes)
			log.WithFields(log.Fields{
				"role":  role,
				"nodes": strings.Join(nodes, ", "),
			}).Debugf("Successful copies")
		}

		failedMsg := utils.FailedPrettifyRolesNodesMap(failedCopies)
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

// VomsProxyInitError is an error returned by any caller trying to generate a vomsProxy
type VomsProxyInitError struct{}

func (v *VomsProxyInitError) Error() string {
	return "Failed to generate VOMS proxy"
}

// Notifications messages
const (
	generalContextErrorString = "Context error for experiment"
	checkKeysErrorString      = `Input file improperly formatted (accounts or nodes don't 
			exist for this experiment). Please check the config file on fifeutilgpvm01.
			 I will skip this experiment for now`
	genericTimeoutErrorString = "Timeout error"
)
