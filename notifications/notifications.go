// Package notifications contains functions needed to send notifications to the relevant stakeholders for the USDC Managed Proxy service
package notifications

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"sync"

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
	msg       string
	adminOnly bool
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
					if err := SendEmail(ctx, nConfig, notificationMsg); err != nil {
						log.WithFields(log.Fields{
							"caller":     "NewManager",
							"experiment": nConfig.Experiment,
						}).Error("Error sending email")
					}
					return
				} else {
					if !n.adminOnly {
						msgSlice = append(msgSlice, n.msg)
					}
					adminMsgSlice = append(adminMsgSlice, n.msg)
				}
			}
		}
	}()
	return c

}

// Sends the admin notifications
func SendAdminNotifications(ctx context.Context, nConfig Config) error {
	var wg *sync.WaitGroup
	var emailErr, slackErr error
	msg := strings.Join(adminMsgSlice, "\n")
	wg.Add(2)

	go func(wg *sync.WaitGroup) {
		defer wg.Done()
		if emailErr = SendEmail(ctx, nConfig, msg); emailErr != nil {
			log.WithField("caller", "SendAdminNotifications").Error("Failed to send admin email")
		}
	}(wg)

	go func(wg *sync.WaitGroup) {
		defer wg.Done()
		if slackErr = SendSlackMessage(ctx, nConfig, msg); slackErr != nil {
			log.WithField("caller", "SendAdminNotifications").Error("Failed to send slack message")
		}
	}(wg)

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
