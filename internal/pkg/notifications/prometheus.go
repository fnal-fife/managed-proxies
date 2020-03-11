package notifications

import (
	"errors"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
)

var (
	promDuration = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "proxy_push",
		Name:      "duration_seconds",
		Help:      "The amount of time it took to run a stage (setup|proxypush|cleanup) of a Managed Proxies Service executable",
	},
		[]string{
			"operation",
			"stage",
		},
	)

	myProxyStoreTime = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "proxy_push",
			Name:      "last_myproxy_timestamp",
			Help:      "The timestamp of the last successful storage of a grid proxy in the configured myproxy server",
		},
		[]string{
			"dn",
		},
	)

	proxyPushTime = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "proxy_push",
			Name:      "last_proxypush_timestamp",
			Help:      "The timestamp of the last successful proxy push of a VOMS proxy to a node",
		},
		[]string{
			"experiment",
			"node",
			"role",
		},
	)
)

// BasicPromPush holds information about a prometheus pushgateway configuration.  It can be passed between processes to report metrics
// to the same Prometheus server.  It is not in and of itself thread-safe, so make sure to either use mutexes or ensure that concurrent
// usage of BasicPromPushes will not cause problems
type BasicPromPush struct {
	P *push.Pusher
	R *prometheus.Registry
}

// RegisterMetrics is a setup function used to register the metrics needed throughout the running of the managed proxy service
func (b BasicPromPush) RegisterMetrics() error {
	if err := b.R.Register(promDuration); err != nil {
		return errors.New("Could not register promTotalDuration metric for monitoring")
	}

	if err := b.R.Register(myProxyStoreTime); err != nil {
		return errors.New("Could not register myProxyStoreTime metric for monitoring")
	}

	if err := b.R.Register(proxyPushTime); err != nil {
		return errors.New("Could not register proxyPushTime metric for monitoring")
	}
	return nil
}

// PushNodeRoleTimestamp sets the value of proxyPushTime to the current time, and pushes that metric to the Pushgateway
// configured in b with the arguments as labels
func (b BasicPromPush) PushNodeRoleTimestamp(experiment, node, role string) error {
	proxyPushTime.WithLabelValues(experiment, node, role).SetToCurrentTime()
	err := b.P.Add()
	return err
}

// PushMyProxyStoreTime sets the value of myProxyStoreTime to the current time, and pushes that metric to the Pushgateway
// configured in b with the arguments as labels
func (b BasicPromPush) PushMyProxyStoreTime(dn string) error {
	myProxyStoreTime.WithLabelValues(dn).SetToCurrentTime()
	err := b.P.Add()
	return err
}

// PushCountErrors creates a Gauge metric, proxyPushErrorCount, sets its value to numErrors, and pushes that to the Pushgateway
// configured in b
func (b BasicPromPush) PushCountErrors(numErrors int) error {
	help := "The number of failed experiments in the last round of proxy pushes"
	name := "proxypush_errors_count"

	proxyPushErrorCount := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: name,
		Help: help,
	})

	if err := b.R.Register(proxyPushErrorCount); err != nil {
		return err
	}
	proxyPushErrorCount.Set(float64(numErrors))

	err := b.P.Add()
	return err
}

// PushPromDuration sets the value of promDuration to the time since start, and pushes that metric to the Pushgateway configured in b
// with the stage argument as a labels
func (b BasicPromPush) PushPromDuration(start time.Time, operation, stage string) error {
	promDuration.WithLabelValues(operation, stage).Set(time.Since(start).Seconds())
	err := b.P.Add()
	return err
}
