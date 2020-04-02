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

var (
	emailDialer gomail.Dialer // gomail dialer to use to send emails
	adminErrors sync.Map      // Store all admin errors here
)

// Notification is an object that holds a message to be sent, as well as the AdminOnly flag to mark it as such.  If AdminOnly is set to true, NewManager will send that message only to adminMsgSlice.
type Notification struct {
	Message    string
	Experiment string
	NotificationType
}

// NotificationType is a flag for the type of message contained in the Notification.  It drives how the Manager behaves.
type NotificationType uint

// SetupError and RunError are the Notification Types that are supported by the notifications.Manager
const (
	SetupError NotificationType = iota + 1
	RunError
)

// Config contains the information needed to send notifications from the proxy push service
type Config struct {
	ConfigInfo map[string]string
	Experiment string
	IsTest     bool
	From       string
	To         []string
	Subject    string
}

// AdminData stores the information needed to generate the Admin notifications
type AdminData struct {
	SetupErrors    []string
	RunErrorsTable string
}

// Manager is simply a channel on which Notification objects can be sent and received
type Manager chan Notification

// SendMessager wraps the SendMessage method
type SendMessager interface {
	SendMessage(context.Context, string, map[string]string) error
}

// SendMessageError indicates that an error occurred sending a message
type SendMessageError struct{ message string }

func (s *SendMessageError) Error() string { return s.message }

// SendMessage sends a message (msg).  The kind of message and how that message is sent is determined
// by the SendMessager, and the ConfigInfo gives supplemental information to send the message.
func SendMessage(ctx context.Context, s SendMessager, msg string, ConfigInfo map[string]string) error {
	err := s.SendMessage(ctx, msg, ConfigInfo)
	if err != nil {
		err := &SendMessageError{"Error sending message"}
		log.Error(err)
		return err
	}
	return nil
}

// Email is an email message configuration
type Email struct {
	From    string
	To      []string
	Subject string
}

// SendMessage sends message as an email based on the Config
func (e *Email) SendMessage(ctx context.Context, message string, ConfigInfo map[string]string) error {
	port, err := strconv.Atoi(ConfigInfo["smtpport"])
	if err != nil {
		return err
	}

	emailDialer = gomail.Dialer{
		Host: ConfigInfo["smtphost"],
		Port: port,
	}

	m := gomail.NewMessage()
	m.SetHeader("From", e.From)
	m.SetHeader("To", e.To...)
	m.SetHeader("Subject", e.Subject)
	m.SetBody("text/plain", message)

	c := make(chan error)
	go func() {
		defer close(c)
		err := emailDialer.DialAndSend(m)
		c <- err
	}()

	select {
	case err := <-c:
		if err != nil {
			log.WithField("recipient", strings.Join(e.To, ", ")).Errorf("Error sending email: %s", err)
		} else {
			log.WithField("recipient", strings.Join(e.To, ", ")).Info("Sent email")
		}
		return err
	case <-ctx.Done():
		err := ctx.Err()
		if err == context.DeadlineExceeded {
			log.WithField("recipient", strings.Join(e.To, ", ")).Error("Error sending email: timeout")
		} else {
			log.WithField("recipient", strings.Join(e.To, ", ")).Errorf("Error sending email: %s", err)
		}
		return err
	}

}

// SlackMessage is a Slack message placeholder
type SlackMessage struct{}

// SendMessage sends message as a Slack message based on the Config
func (s *SlackMessage) SendMessage(ctx context.Context, message string, ConfigInfo map[string]string) error {
	if e := ctx.Err(); e != nil {
		log.Errorf("Error sending slack message: %s", e)
		return e
	}

	if message == "" {
		log.Warn("Slack message is empty.  Will not attempt to send it")
		return nil
	}

	msg := []byte(fmt.Sprintf(`{"text": "%s"}`, strings.Replace(message, "\"", "\\\"", -1)))
	req, err := http.NewRequest("POST", ConfigInfo["slack_alerts_url"], bytes.NewBuffer(msg))
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
