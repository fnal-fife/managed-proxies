package notifications

import (
	"errors"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
)

var (
	promTotalDuration = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "proxy_push_total_seconds",
		Help: "The total amount of time it took to run the entire Managed Proxies Service script",
	})
	proxyPushTime = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "proxy_push",
			Name:      "last_timestamp",
			Help:      "The timestamp of the last successful proxy push of a VOMS proxy to a node",
		},
		[]string{
			"experiment",
			"node",
			"role",
		},
	)
)

// Comment
type BasicPromPush struct {
	P *push.Pusher
	R *prometheus.Registry
}

func (b BasicPromPush) RegisterMetrics() error {
	if err := b.R.Register(promTotalDuration); err != nil {
		return errors.New("Could not register promTotalDuration metric for monitoring")
	}

	if err := b.R.Register(proxyPushTime); err != nil {
		return errors.New("Could not register proxyPushTime metric for monitoring")
	}
	return nil
}

func (b BasicPromPush) PushNodeRoleTimestamp(experiment, node, role string) error {
	// Ensure that "-" in our config names are okay for Prometheus
	// experiment = strings.Replace(experiment, "-", "", -1)
	// node = strings.Replace(node, "-", "", -1)
	// role = strings.Replace(role, "-", "", -1)

	// help := "The timestamp of the last successful proxy push of role " + role + " to node " + node + " for experiment " + experiment
	// name := experiment + "_" + node + "_" + role

	// proxyPushTime := prometheus.NewGauge(prometheus.GaugeOpts{
	// 	Name: name,
	// 	Help: help,
	// })
	// b.R.MustRegister(proxyPushTime)
	proxyPushTime.WithLabelValues(experiment, node, role).SetToCurrentTime()

	err := b.P.Add()
	return err
}

func (b BasicPromPush) PushCountErrors(numErrors int) error {
	help := "The number of failed experiments in the last round of proxy pushes"

	proxyPushErrorCount := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "proxypush_num_errors",
		Help: help,
	})

	b.R.MustRegister(proxyPushErrorCount)
	proxyPushErrorCount.Set(float64(numErrors))

	err := b.P.Add()
	return err
}

func (b BasicPromPush) PushPromTotalDuration(start time.Time) error {
	promTotalDuration.Set(time.Since(start).Seconds())

	b.R.MustRegister(promTotalDuration)
	err := b.P.Add()
	return err
}
