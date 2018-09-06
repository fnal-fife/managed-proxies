package proxyPushLogger

import (
	"errors"
	"fmt"

	"github.com/rifflock/lfshook"
	"github.com/sirupsen/logrus"
)

const exptErrorFilenamef string = "proxy_push_error_experiment_%s.log"

// LogsConfig is a map of the location of the logfiles
type LogsConfig map[string]string

// New sets up the logging for either an experiment or the general logger
func New(expt string, lConfig LogsConfig) *logrus.Entry {
	var log = logrus.New()

	log.SetLevel(logrus.DebugLevel)

	log.AddHook(lfshook.NewHook(lfshook.PathMap{
		logrus.DebugLevel: lConfig["debugfile"],
		logrus.InfoLevel:  lConfig["debugfile"],
		logrus.WarnLevel:  lConfig["debugfile"],
		logrus.ErrorLevel: lConfig["debugfile"],
		logrus.FatalLevel: lConfig["debugfile"],
		logrus.PanicLevel: lConfig["debugfile"],
	}, &logrus.TextFormatter{FullTimestamp: true}))

	log.AddHook(lfshook.NewHook(lfshook.PathMap{
		logrus.InfoLevel:  lConfig["logfile"],
		logrus.WarnLevel:  lConfig["logfile"],
		logrus.ErrorLevel: lConfig["logfile"],
		logrus.FatalLevel: lConfig["logfile"],
		logrus.PanicLevel: lConfig["logfile"],
	}, &logrus.TextFormatter{FullTimestamp: true}))

	// General Error Log that will get sent to Admins
	log.AddHook(lfshook.NewHook(lfshook.PathMap{
		logrus.ErrorLevel: lConfig["errfile"],
		logrus.FatalLevel: lConfig["errfile"],
		logrus.PanicLevel: lConfig["errfile"],
	}, new(ExptErrorFormatter)))

	if expt != "" {
		exptErrorFilename := fmt.Sprintf(exptErrorFilenamef, expt)

		// Experiment-specific error log that gets emailed if populated
		log.AddHook(lfshook.NewHook(lfshook.PathMap{
			logrus.ErrorLevel: exptErrorFilename,
			logrus.FatalLevel: exptErrorFilename,
			logrus.PanicLevel: exptErrorFilename,
		}, new(ExptErrorFormatter)))

		return log.WithField("experiment", expt)
	}
	return logrus.NewEntry(log)
}

// ExptErrorFormatter is a struct that implements the logrus.Formatter interface.  It's for experiment logs that will be emailed to experiments in
// case of error
type ExptErrorFormatter struct {
}

// Format defines how any logger using the ExptErrorFormatter should emit its
// log records.  We expect to see [date] [experiment] [level] [message]
func (f *ExptErrorFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	var expt string
	if val, ok := entry.Data["experiment"]; ok {
		if expt, ok = val.(string); !ok {
			return nil, errors.New("entry.Data[\"experiment\"] Failed type assertion")
		}
	} else {
		expt = "No Experiment"
	}

	logLine := fmt.Sprintf("[%s] [%s] [%s]: %s", entry.Time, expt, entry.Level, entry.Message)
	logByte := []byte(logLine)
	return append(logByte, '\n'), nil
}

// This returns the filename of the experiment error file.  It's only a func so that other
// packages can use it
func GetExptErrorLogfileName(expt string) string {
	return fmt.Sprintf(exptErrorFilenamef, expt)
}
