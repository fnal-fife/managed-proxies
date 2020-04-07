package checkcerts

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"text/template"
	"time"

	log "github.com/sirupsen/logrus"

	"cdcvs.fnal.gov/discompsupp/ken_proxy_push/v3/notifications"
)

// CertExpirationNotification holds information about a service certificate, its expiration date, and whether or not the admins should be warned about that certificate's expiration.
type CertExpirationNotification struct {
	Account, DN string
	DaysLeft    int
	Warn        bool
}

// SendCertAlarms sends reports to admins about certificate expiration.  It can be adapted to many templates, as long these templates range over the items in cSlice.
func SendCertAlarms(ctx context.Context, nConfig notifications.Config, cSlice []CertExpirationNotification, templateFileName string) error {
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
		email := &notifications.Email{
			From:    nConfig.From,
			To:      nConfig.To,
			Subject: nConfig.Subject,
		}
		if emailErr = notifications.SendMessage(ctx, email, b.String(), nConfig.ConfigInfo); emailErr != nil {
			log.WithField("caller", "SendCertAlarms").Error("Failed to send admin email")
		}
	}(&wg)

	go func(wg *sync.WaitGroup) {
		defer wg.Done()
		s := &notifications.SlackMessage{}
		if slackErr = notifications.SendMessage(ctx, s, b.String(), nConfig.ConfigInfo); slackErr != nil {
			log.WithField("caller", "SendCertAlarms").Error("Failed to send slack message")
		}
	}(&wg)

	wg.Wait()

	if emailErr != nil || slackErr != nil {
		return errors.New("Sending checkcerts notifications failed.  Please see logs")
	}
	return nil

}
