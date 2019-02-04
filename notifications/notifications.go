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

// Notification is an object that holds a message to be sent, as well as the AdminOnly flag to mark it as such.  If AdminOnly is set to true, NewManager will send that message only to adminMsgSlice.
type Notification struct {
	Msg       string
	AdminOnly bool
}

// Manager is simply a channel on which Notification objects can be sent and received
type Manager chan Notification

// SendEmail sends an email based on nConfig
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

	m := gomail.NewMessage()
	m.SetHeader("From", nConfig.From)
	m.SetHeader("To", nConfig.To...)
	m.SetHeader("Subject", nConfig.Subject)
	m.SetBody("text/plain", msg)

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

	if message == "" {
		log.Warn("Slack message is empty.  Will not attempt to send it")
		return nil
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
