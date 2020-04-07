package notifications

import (
	"context"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
	gomail "gopkg.in/gomail.v2"
)

var emailDialer gomail.Dialer // gomail dialer to use to send emails

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
