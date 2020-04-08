package utils

import (
	"errors"
	"fmt"
	"os/exec"
	"os/user"
	"regexp"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"

	"cdcvs.fnal.gov/discompsupp/ken_proxy_push/v4/notifications"
)

// EmailRegexp is the regexp that all email addresses must satisfy
var EmailRegexp = regexp.MustCompile(`^[\w\._%+-]+@[\w\.-]+\.\w{2,}$`)

// SetAdminEmail sets the notifications config objects' From and To fields to the config file's admin value
func SetAdminEmail(pnConfig *notifications.Config) {
	var toEmail string
	pnConfig.From = pnConfig.ConfigInfo["admin_email"]

	if viper.GetString("admin") != "" {
		toEmail = viper.GetString("admin")
	} else {
		toEmail = pnConfig.ConfigInfo["admin_email"]
	}

	pnConfig.To = []string{toEmail}
	log.Debug("Set notifications config email values to admin values")
	return
}

// CheckUser makes sure that the user running the executable is the authorized user.
func CheckUser(authuser string) error {
	cuser, err := user.Current()
	if err != nil {
		return errors.New("Could not lookup current user.  Exiting")
	}
	log.Debug("Running script as ", cuser.Username)
	if cuser.Username != authuser {
		return fmt.Errorf("This must be run as %s.  Trying to run as %s", authuser, cuser.Username)
	}
	return nil
}

// CheckForExecutables takes a map of executables of the form {"name_of_executable": "whatever"} and
// checks if each executable is in $PATH.  If so, it saves the path in the map.  If not, it returns an error
func CheckForExecutables(exeMap map[string]string) error {
	for exe := range exeMap {
		pth, err := exec.LookPath(exe)
		if err != nil {
			return fmt.Errorf("%s was not found in $PATH", exe)
		}
		exeMap[exe] = pth
	}
	return nil
}
