package notifications

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"math"
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
		msgSlice := make([]string, 0) // Collect experiment-specific messages
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
					if !nConfig.IsTest {
						if len(msgSlice) > 0 {
							if err := SendExperimentEmail(ctx, nConfig, msgSlice); err != nil {
								log.WithFields(log.Fields{
									"caller":     "NewManager",
									"experiment": nConfig.Experiment,
								}).Error("Error sending email")
							}
						}
					}
					return
				}
				// Direct the message as needed
				if !n.AdminOnly {
					msgSlice = append(msgSlice, n.Msg)
				}
				adminMsgSlice = append(adminMsgSlice, n.Msg)
			}
		}
	}()

	return c
}

// SendExperimentEmail sends an experiment-specific error message email based on nConfig.  It expects a valid template file configured at notifications.experiment_template
func SendExperimentEmail(ctx context.Context, nConfig Config, errorsSlice []string) (err error) {
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
			"caller":     "SendAdminNotifications",
			"experiment": nConfig.Experiment,
		}).Errorf("Could not read experiment error template file: %s", err)
		return err
	}

	timestamp := time.Now().Format(time.RFC822)
	exptEmailTemplate := template.Must(template.New("exptEmail").Parse(string(templateData)))
	if err = exptEmailTemplate.Execute(&b, struct {
		Timestamp     string
		ErrorMessages []string
	}{
		Timestamp:     timestamp,
		ErrorMessages: errorsSlice,
	}); err != nil {
		log.WithFields(log.Fields{
			"caller":     "SendAdminNotifications",
			"experiment": nConfig.Experiment,
		}).Errorf("Failed to execute experiment email template: %s", err)
		return err
	}

	go func(wg *sync.WaitGroup) {
		defer wg.Done()
		if err = SendEmail(ctx, nConfig, b.String()); err != nil {
			log.WithFields(log.Fields{
				"caller":     "SendAdminNotifications",
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

	if len(adminMsgSlice) == 0 {
		if nConfig.IsTest {
			slackMsg := "Test run completed successfully"
			if slackErr = SendSlackMessage(ctx, nConfig, slackMsg); slackErr != nil {
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
		Timestamp     string
		Operation     string
		ErrorMessages []string
	}{
		Timestamp:     timestamp,
		Operation:     operation,
		ErrorMessages: adminMsgSlice,
	}); err != nil {
		log.WithField("caller", "SendAdminNotifications").Errorf("Failed to execute admin email template: %s", err)
		return err
	}

	go func(wg *sync.WaitGroup) {
		defer wg.Done()
		if emailErr = SendEmail(ctx, nConfig, b.String()); emailErr != nil {
			log.WithField("caller", "SendAdminNotifications").Error("Failed to send admin email")
		}
	}(&wg)

	go func(wg *sync.WaitGroup) {
		defer wg.Done()
		if slackErr = SendSlackMessage(ctx, nConfig, b.String()); slackErr != nil {
			log.WithField("caller", "SendAdminNotifications").Error("Failed to send slack message")
		}
	}(&wg)

	wg.Wait()

	if emailErr != nil || slackErr != nil {
		return errors.New("Sending admin notifications failed.  Please see logs")
	}
	return nil
}

// CertExpirationNotification holds information about a service certificate, its expiration date, and whether or not the admins should be warned about that certificate's expiration.
type CertExpirationNotification struct {
	Account, DN string
	DaysLeft    int
	Warn        bool
}

// SendCertAlarms sends reports to admins about certificate expiration.  It can be adapted to many templates, as long these templates range over the items in cSlice.
func SendCertAlarms(ctx context.Context, nConfig Config, cSlice []CertExpirationNotification, templateFileName string) error {
	var wg sync.WaitGroup
	var emailErr, slackErr error
	var b strings.Builder

	wg.Add(2)

	timeLeftConfig, err := time.ParseDuration(nConfig.ConfigInfo["expireWarningCutoff"])
	if err != nil {
		log.WithField("caller", "SendCertAlarms").Errorf("Failed to parse time left from configuration struct %s", err)
	}
	numDaysConfig := int(math.Round(timeLeftConfig.Hours() / 24.0))

	certAlarmTemplate := template.Must(template.ParseFiles(templateFileName))

	templateInput := struct {
		ConfigNumDaysLeft int
		CSlice            []CertExpirationNotification
	}{
		ConfigNumDaysLeft: numDaysConfig,
		CSlice:            cSlice,
	}

	if err = certAlarmTemplate.Execute(&b, templateInput); err != nil {
		log.WithField("caller", "SendCertAlarms").Errorf("Failed to execute checkcerts email template: %s", err)
		return err
	}

	if nConfig.IsTest {
		fmt.Println("Test mode - will print out message and exit without sending it")
		fmt.Printf("\n%s\n", b.String())
		return nil
	}

	go func(wg *sync.WaitGroup) {
		defer wg.Done()
		if emailErr = SendEmail(ctx, nConfig, b.String()); emailErr != nil {
			log.WithField("caller", "SendCertAlarms").Error("Failed to send admin email")
		}
	}(&wg)

	go func(wg *sync.WaitGroup) {
		defer wg.Done()
		if slackErr = SendSlackMessage(ctx, nConfig, b.String()); slackErr != nil {
			log.WithField("caller", "SendCertAlarms").Error("Failed to send slack message")
		}
	}(&wg)

	wg.Wait()

	if emailErr != nil || slackErr != nil {
		return errors.New("Sending checkcerts notifications failed.  Please see logs")
	}
	return nil

}
