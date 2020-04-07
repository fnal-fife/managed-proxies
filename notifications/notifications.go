// Package notifications contains functions needed to send notifications to the relevant stakeholders for the USDC Managed Proxy service
package notifications

import (
	"context"
	"sync"

	log "github.com/sirupsen/logrus"
)

var (
	adminErrors sync.Map // Store all admin errors here
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
