package notifications

import (
	"context"
	"errors"
	"io/ioutil"
	"strings"
	"sync"
	"text/template"
	"time"

	log "github.com/sirupsen/logrus"
)

// NewManager returns a Manager channel for callers to send Notifications on.  It will collect messages, and when Manager is closed, will send emails, depending on nConfig.IsTest
func NewManager(ctx context.Context, wg *sync.WaitGroup, nConfig Config) Manager {
	c := make(Manager)

	go func() {
		//msgSlice := make([]string, 0) // Collect experiment-specific messages
		var exptErrorTable string
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				if e := ctx.Err(); e == context.DeadlineExceeded {
					log.WithFields(log.Fields{
						"caller":     "NewManager",
						"experiment": nConfig.Experiment,
					}).Error("Timeout exceeded in notification Manager")

				} else {
					log.WithFields(log.Fields{
						"caller":     "NewManager",
						"experiment": nConfig.Experiment,
					}).Error(e)
				}
				return

			case n, chanOpen := <-c:
				// Channel is closed --> send emails
				if !chanOpen {
					// If running for real, send the experiment email
					if nConfig.IsTest {
						return
					}
					if len(exptErrorTable) > 0 {
						if err := SendExperimentEmail(ctx, nConfig, exptErrorTable); err != nil {
							log.WithFields(log.Fields{
								"caller":     "NewManager",
								"experiment": nConfig.Experiment,
							}).Error("Error sending email")
						}
					}
					return
				}
				// Channel is open: direct the message as needed
				switch n.NotificationType {
				case SetupError:
					addSetupErrorToAdminErrors(&n)
				case RunError:
					exptErrorTable = n.Message
					addTableToAdminErrors(&n)
				}

			}
		}
	}()

	return c
}

// SendExperimentEmail sends an experiment-specific error message email based on nConfig.  It expects a valid template file configured at notifications.experiment_template
func SendExperimentEmail(ctx context.Context, nConfig Config, errorTable string) (err error) {
	var wg sync.WaitGroup
	var b strings.Builder

	wg.Add(1)

	experimentTemplateFile, ok := nConfig.ConfigInfo["experiment_template"]
	if !ok {
		err := "experiment_template is not configured in the notifications section of the configuration file"
		log.WithField("caller", "SendAdminNotifications").Error("experiment_template is not configured in the notifications section of the configuration file")
		return errors.New(err)
	}

	templateData, err := ioutil.ReadFile(experimentTemplateFile)
	if err != nil {
		log.WithFields(log.Fields{
			"caller":     "SendExperimentEmail",
			"experiment": nConfig.Experiment,
		}).Errorf("Could not read experiment error template file: %s", err)
		return err
	}

	timestamp := time.Now().Format(time.RFC822)
	exptEmailTemplate := template.Must(template.New("exptEmail").Parse(string(templateData)))
	if err = exptEmailTemplate.Execute(&b, struct {
		Timestamp  string
		ErrorTable string
	}{
		Timestamp:  timestamp,
		ErrorTable: errorTable,
	}); err != nil {
		log.WithFields(log.Fields{
			"caller":     "SendExperimentEmail",
			"experiment": nConfig.Experiment,
		}).Errorf("Failed to execute experiment email template: %s", err)
		return err
	}

	go func(wg *sync.WaitGroup) {
		defer wg.Done()
		email := &Email{
			From:    nConfig.From,
			To:      nConfig.To,
			Subject: nConfig.Subject,
		}

		if nConfig.IsTest {
			log.Info("This is a test.  Not sending email")
			err = nil
			return
		}

		if err = SendMessage(ctx, email, b.String(), nConfig.ConfigInfo); err != nil {
			log.WithFields(log.Fields{
				"caller":     "SendExperimentEmail",
				"experiment": nConfig.Experiment,
			}).Error("Failed to send experiment email")
		}
	}(&wg)

	wg.Wait()
	return err
}

// SendAdminNotifications sends admin messages via email and Slack that have been collected in adminMsgSlice. It expects a valid template file configured at nConfig.ConfigInfo["admin_template"].
func SendAdminNotifications(ctx context.Context, nConfig Config, operation string) error {
	var wg sync.WaitGroup
	var emailErr, slackErr error
	var b strings.Builder
	adminErrorsMap := make(map[string]AdminData)

	// Convert from sync.Map to Map
	adminErrors.Range(func(expt, adminData interface{}) bool {
		e, ok := expt.(string)
		if !ok {
			log.Panic("Improper key in admin notifications map.")
		}
		a, ok := adminData.(AdminData)
		if !ok {
			log.Panic("Invalid admin data stored for notification")
		}
		adminErrorsMap[e] = a
		return true
	})

	if len(adminErrorsMap) == 0 {
		if nConfig.IsTest {
			s := &SlackMessage{}
			msg := "Test run completed successfully"
			if slackErr = SendMessage(ctx, s, msg, nConfig.ConfigInfo); slackErr != nil {
				log.WithField("caller", "SendAdminNotifications").Error("Failed to send slack message")
			}
			return slackErr
		}
		log.WithField("caller", "SendAdminNotifications").Debug("No errors to send")
		return nil
	}

	adminTemplateFile, ok := nConfig.ConfigInfo["admin_template"]
	if !ok {
		err := "admin_template is not configured in the notifications section of the configuration file"
		log.WithField("caller", "SendAdminNotifications").Error("admin_template is not configured in the notifications section of the configuration file")
		return errors.New(err)
	}

	wg.Add(2)

	timestamp := time.Now().Format(time.RFC822)
	templateData, err := ioutil.ReadFile(adminTemplateFile)
	if err != nil {
		log.WithField("caller", "SendAdminNotifications").Errorf("Could not read admin error template file: %s", err)
		return err
	}
	adminTemplate := template.Must(template.New("admin").Parse(string(templateData)))

	if err = adminTemplate.Execute(&b, struct {
		Timestamp   string
		Operation   string
		AdminErrors map[string]AdminData
	}{
		Timestamp:   timestamp,
		Operation:   operation,
		AdminErrors: adminErrorsMap,
	}); err != nil {
		log.WithField("caller", "SendAdminNotifications").Errorf("Failed to execute admin email template: %s", err)
		return err
	}

	go func(wg *sync.WaitGroup) {
		defer wg.Done()
		email := &Email{
			From:    nConfig.From,
			To:      nConfig.To,
			Subject: nConfig.Subject,
		}

		if nConfig.IsTest {
			log.Info("This is a test.  Not sending email")
			err = nil
			return
		}

		if emailErr = SendMessage(ctx, email, b.String(), nConfig.ConfigInfo); emailErr != nil {
			log.WithField("caller", "SendAdminNotifications").Error("Failed to send admin email")
		}
	}(&wg)

	go func(wg *sync.WaitGroup) {
		defer wg.Done()
		s := &SlackMessage{}
		if slackErr = SendMessage(ctx, s, b.String(), nConfig.ConfigInfo); slackErr != nil {
			log.WithField("caller", "SendAdminNotifications").Error("Failed to send slack message")
		}
	}(&wg)

	wg.Wait()

	if emailErr != nil || slackErr != nil {
		return errors.New("Sending admin notifications failed.  Please see logs")
	}
	return nil
}

// addSetupErrorToAdminErrors adds a Notification.Message to the adminErrors[Notification.Experiment].SetupErrors slice if the Notifications.NotificationType is SetupError
func addSetupErrorToAdminErrors(n *Notification) {
	// is the expt key there?
	// If so, grab the AdminData struct, set it aside
	// Add to that AdminData stuff
	if actual, loaded := adminErrors.LoadOrStore(
		n.Experiment,
		AdminData{
			SetupErrors: []string{n.Message},
		},
	); loaded {
		if adminData, ok := actual.(AdminData); !ok {
			log.Panic("Invalid data stored in admin errors map.")
		} else {
			adminData.SetupErrors = append(adminData.SetupErrors, n.Message)
			adminErrors.Store(n.Experiment, adminData)
		}
	}
	return
}

// addTableToAdminErrors populates adminErrors[Notification.Experiment].RunErrorsTable with the Notification.Message if the Notifications.NotificationType is RunError
func addTableToAdminErrors(n *Notification) {
	if actual, loaded := adminErrors.LoadOrStore(
		n.Experiment,
		AdminData{RunErrorsTable: n.Message},
	); loaded {
		if adminData, ok := actual.(AdminData); !ok {
			log.Panic("Invalid data stored in admin errors map.")
		} else {
			adminData.RunErrorsTable = n.Message
			adminErrors.Store(n.Experiment, adminData)
		}
	}
	return
}
