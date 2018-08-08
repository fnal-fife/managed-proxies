// Package notifications contains functions needed to send notifications to the relevant stakeholders for the USDC Managed Proxy service
package notifications

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"

	//"github.com/spf13/viper"
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
		if e == nil {
			fmt.Printf("Sent emails to %s\n", strings.Join(nConfig.To, ", "))
		}
		return e
	case <-ctx.Done():
		e := ctx.Err()
		if e != context.DeadlineExceeded {
			return fmt.Errorf("Hit timeout attempting to send email to %s", nConfig.Experiment)
		}
		return e
	}
}

// SendSlackMessage sends an HTTP POST request to a URL specified in the config file.
func SendSlackMessage(ctx context.Context, nConfig Config, message string) error {
	if e := ctx.Err(); e != nil {
		return e
	}

	msg := []byte(fmt.Sprintf(`{"text": "%s"}`, strings.Replace(message, "\"", "\\\"", -1)))
	req, err := http.NewRequest("POST", nConfig.ConfigInfo["slack_alerts_url"], bytes.NewBuffer(msg))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(ctx)

	client := http.DefaultClient
	resp, err := client.Do(req)
	if err != nil {
		return err
	}

	// This should be redundant, but just in case the timeout before didn't trigger.
	if e := ctx.Err(); e != nil {
		return e
	}

	defer resp.Body.Close()

	// Parse the response to make sure we're good
	if resp.StatusCode != http.StatusOK {
		body, _ := ioutil.ReadAll(resp.Body)
		errmsg := fmt.Errorf("Could not send Slack message. "+
			"Slack Response Status: %s\nSlack Response Headers: %s\nSlack Response Body: %s",
			resp.Status, resp.Header, string(body))
		return errmsg
	}
	fmt.Println("Slack message sent")
	return nil
}
