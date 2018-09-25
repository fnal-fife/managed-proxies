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

	"github.com/sirupsen/logrus"
	gomail "gopkg.in/gomail.v2"
)

//var emailDialer = gomail.Dialer{Host: "localhost", Port: 25} // gomail dialer to use to send emails
var emailDialer gomail.Dialer // gomail dialer to use to send emails

// Config contains the information needed to send notifications from the proxy push service
type Config struct {
	ConfigInfo map[string]string
	From       string
	To         []string
	Subject    string
	Experiment string
	Logger     *logrus.Entry
}

// addHelperMessage tacks on the help text we want experiments to get.
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

// SendEmail sends emails to both experiments and admins, depending on the input (exptName = "" gives admin email).
// func SendEmail(ctx context.Context, nConfig Config, exptName, msg string) error {
func SendEmail(ctx context.Context, nConfig Config, msg string) error {
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
			nConfig.Logger.WithField("recipient", strings.Join(nConfig.To, ", ")).Errorf("Error sending email: %s", e)
		} else {
			nConfig.Logger.WithField("recipient", strings.Join(nConfig.To, ", ")).Info("Sent email")
		}
		return e
	case <-ctx.Done():
		e := ctx.Err()
		if e == context.DeadlineExceeded {
			nConfig.Logger.WithField("recipient", strings.Join(nConfig.To, ", ")).Error("Error sending email: timeout")
		} else {
			nConfig.Logger.WithField("recipient", strings.Join(nConfig.To, ", ")).Errorf("Error sending email: %s", e)
		}
		return e
	}
}

// SendSlackMessage sends an HTTP POST request to a URL specified in the config file.
func SendSlackMessage(ctx context.Context, nConfig Config, message string) error {
	if e := ctx.Err(); e != nil {
		nConfig.Logger.Errorf("Error sending slack message: %s", e)
		return e
	}

	msg := []byte(fmt.Sprintf(`{"text": "%s"}`, strings.Replace(message, "\"", "\\\"", -1)))
	req, err := http.NewRequest("POST", nConfig.ConfigInfo["slack_alerts_url"], bytes.NewBuffer(msg))
	if err != nil {
		nConfig.Logger.Errorf("Error sending slack message: %s", err)
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(ctx)

	client := http.DefaultClient
	resp, err := client.Do(req)
	if err != nil {
		nConfig.Logger.Errorf("Error sending slack message: %s", err)
		return err
	}

	// This should be redundant, but just in case the timeout before didn't trigger.
	if e := ctx.Err(); e != nil {
		nConfig.Logger.Errorf("Error sending slack message: %s", e)
		return e
	}

	defer resp.Body.Close()

	// Parse the response to make sure we're good
	if resp.StatusCode != http.StatusOK {
		body, _ := ioutil.ReadAll(resp.Body)
		err := errors.New("Could not send slack message")
		nConfig.Logger.WithFields(logrus.Fields{
			"response status":  resp.Status,
			"response headers": resp.Header,
			"response body":    string(body),
		}).Error(err)
		return err
	}
	nConfig.Logger.Info("Slack message sent")
	return nil
}
