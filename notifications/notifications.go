// Package notifications contains functions needed to send notifications to the relevant stakeholders for the USDC Managed Proxy service
package notifications

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"path"
	"strconv"
	"strings"
	"sync"
	"text/template"

	log "github.com/sirupsen/logrus"
	gomail "gopkg.in/gomail.v2"
)

//var emailDialer = gomail.Dialer{Host: "localhost", Port: 25} // gomail dialer to use to send emails
var (
	emailDialer   gomail.Dialer // gomail dialer to use to send emails
	adminMsgSlice []string
)

// Config contains the information needed to send notifications from the proxy push service
type Config struct {
	ConfigInfo map[string]string
	From       string
	To         []string
	Subject    string
	Experiment string
	IsTest     bool
}

type Notification struct {
	Msg       string
	AdminOnly bool
}

type Manager chan Notification

func NewManager(ctx context.Context, wg *sync.WaitGroup, nConfig Config) Manager {
	c := make(Manager)

	go func() {
		msgSlice := make([]string, 0)
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
				if !chanOpen {
					notificationMsg := strings.Join(msgSlice, "\n")
					if !nConfig.IsTest {
						if len(msgSlice) > 0 {

							// TODO  This should now be sendExptEmail
							if err := SendEmail(ctx, nConfig, notificationMsg); err != nil {
								log.WithFields(log.Fields{
									"caller":     "NewManager",
									"experiment": nConfig.Experiment,
								}).Error("Error sending email")
							}
						}
					}
					return
				} else {
					if !n.AdminOnly {
						msgSlice = append(msgSlice, n.Msg)
					}
					adminMsgSlice = append(adminMsgSlice, n.Msg)
				}
			}
		}
	}()

	go func() {
		wg.Wait()
		if err := SendAdminNotifications(ctx, nConfig); err != nil {
			log.WithFields(log.Fields{
				"caller":     "NewManager",
				"experiment": nConfig.Experiment,
			}).Error("Error sending Admin Notifications")
		}
	}()

	return c

}

// Sends an experiment email
func SendExperimentEmail(ctx context.Context, nConfig Config, msg string) (err error) {
	// TODO  model this after the next bit
	var wg sync.WaitGroup
	var b strings.Builder

	wg.Add(1)

	// Read in template

	templateFileName := path.Join(nConfig.ConfigInfo["templateDir"], "proxyPushExperimentError.txt")
	templateData, err := ioutil.ReadFile(templateFileName)
	if err != nil {
		log.WithFields(log.Fields{
			"caller":     "SendAdminNotifications",
			"experiment": nConfig.Experiment,
		}).Errorf("Could not read experiment error template file: %s", err)
		return err
	}

	exptEmailTemplate := template.Must(template.New("exptEmail").Parse(string(templateData)))
	if err = exptEmailTemplate.Execute(&b, struct {
		ErrorMessages string
	}{
		ErrorMessages: msg,
	}); err != nil {
		log.WithFields(log.Fields{
			"caller":     "SendAdminNotifications",
			"experiment": nConfig.Experiment,
		}).Error("Failed to execute experiment email template")
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
	// Return error status
	wg.Wait()

	return err
}

// Sends the admin notifications
func SendAdminNotifications(ctx context.Context, nConfig Config) error {
	// TODO  Need to return error early if issue.  See above for example
	var wg sync.WaitGroup
	var emailErr, slackErr error
	var b strings.Builder
	var slackMsg string

	msg := strings.Join(adminMsgSlice, "\n")
	wg.Add(2)

	templateFileName := path.Join(nConfig.ConfigInfo["templateDir"], "adminErrors.txt")
	templateData, err := ioutil.ReadFile(templateFileName)
	if err != nil {
		log.WithField("caller", "SendAdminNotifications").Errorf("Could not read admin error template file: %s", err)
	}
	adminTemplate := template.Must(template.New("admin").Parse(string(templateData)))

	if err = adminTemplate.Execute(&b, struct {
		ErrorMessages string
	}{
		ErrorMessages: msg,
	}); err != nil {
		log.WithField("caller", "SendAdminNotifications").Error("Failed to execute admin email template")
	}

	go func(wg *sync.WaitGroup) {
		defer wg.Done()
		if emailErr = SendEmail(ctx, nConfig, b.String()); emailErr != nil {
			log.WithField("caller", "SendAdminNotifications").Error("Failed to send admin email")
		}
	}(&wg)

	if msg == "" {
		slackMsg = "Test run completed successfully"
	} else {
		slackMsg = b.String()
	}

	go func(wg *sync.WaitGroup) {
		defer wg.Done()
		if slackErr = SendSlackMessage(ctx, nConfig, slackMsg); slackErr != nil {
			log.WithField("caller", "SendAdminNotifications").Error("Failed to send slack message")
		}
	}(&wg)

	wg.Wait()

	if emailErr != nil || slackErr != nil {
		return errors.New("Sending admin notifications failed.  Please see logs.")
	}
	return nil
}

// SendEmail sends emails to both experiments and admins, depending on the input (exptName = "" gives admin email).
// func SendEmail(ctx context.Context, nConfig Config, exptName, msg string) error {
func SendEmail(ctx context.Context, nConfig Config, msg string) error {
	if nConfig.IsTest {
		log.Info("This is a test.  Not sending email")
		return nil
	}

	port, err := strconv.Atoi(nConfig.ConfigInfo["smtpport"])
	if err != nil {
		return err
	}

	emailDialer = gomail.Dialer{
		Host: nConfig.ConfigInfo["smtphost"],
		Port: port,
	}
	message := addHelperMessage(msg, nConfig.Experiment)

	m := gomail.NewMessage()
	m.SetHeader("From", nConfig.From)
	m.SetHeader("To", nConfig.To...)
	m.SetHeader("Subject", nConfig.Subject)
	m.SetBody("text/plain", message)

	c := make(chan error)
	go func() {
		defer close(c)
		err := emailDialer.DialAndSend(m)
		c <- err
	}()

	select {
	case e := <-c:
		if e != nil {
			log.WithField("recipient", strings.Join(nConfig.To, ", ")).Errorf("Error sending email: %s", e)
		} else {
			log.WithField("recipient", strings.Join(nConfig.To, ", ")).Info("Sent email")
		}
		return e
	case <-ctx.Done():
		e := ctx.Err()
		if e == context.DeadlineExceeded {
			log.WithField("recipient", strings.Join(nConfig.To, ", ")).Error("Error sending email: timeout")
		} else {
			log.WithField("recipient", strings.Join(nConfig.To, ", ")).Errorf("Error sending email: %s", e)
		}
		return e
	}
}

// SendSlackMessage sends an HTTP POST request to a URL specified in the config file.
func SendSlackMessage(ctx context.Context, nConfig Config, message string) error {
	if e := ctx.Err(); e != nil {
		log.Errorf("Error sending slack message: %s", e)
		return e
	}
	// TODO:  Add that if message is "", return immediately.  that way we can avoid a 400 HTTP error

	msg := []byte(fmt.Sprintf(`{"text": "%s"}`, strings.Replace(message, "\"", "\\\"", -1)))
	req, err := http.NewRequest("POST", nConfig.ConfigInfo["slack_alerts_url"], bytes.NewBuffer(msg))
	if err != nil {
		log.Errorf("Error sending slack message: %s", err)
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(ctx)

	client := http.DefaultClient
	resp, err := client.Do(req)
	if err != nil {
		log.Errorf("Error sending slack message: %s", err)
		return err
	}

	// This should be redundant, but just in case the timeout before didn't trigger.
	if e := ctx.Err(); e != nil {
		log.Errorf("Error sending slack message: %s", e)
		return e
	}

	defer resp.Body.Close()

	// Parse the response to make sure we're good
	if resp.StatusCode != http.StatusOK {
		body, _ := ioutil.ReadAll(resp.Body)
		err := errors.New("Could not send slack message")
		log.WithFields(log.Fields{
			"response status":  resp.Status,
			"response headers": resp.Header,
			"response body":    string(body),
		}).Error(err)
		return err
	}
	log.Info("Slack message sent")
	return nil
}

func addHelperMessage(msg, expt string) string {
	help := "\n\nWe've compiled a list of common errors here: " +
		"https://cdcvs.fnal.gov/redmine/projects/fife/wiki/Common_errors_with_Managed_Proxies_Service. " +
		"\n\nIf you have any questions or comments about these emails, " +
		"please open a Service Desk ticket to the Distributed Computing " +
		"Support group."

	if expt == "" {
		return msg
	}
	return msg + help
}
