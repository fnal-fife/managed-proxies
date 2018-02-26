package notifications

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
)

var (
	promTotalDuration = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "proxy_push_total_seconds",
		Help: "The total amount of time it took to run the entire Managed Proxies Service script",
	})
)

// Comment
type BasicPromPush struct {
	P *push.Pusher
	R *prometheus.Registry
}

func (b BasicPromPush) PushNodeTimestamp(node, role string) error {
	help := "The timestamp of the last successful proxy push of role " + role + " to node " + node
	name := node + "_" + role

	proxyPushTime := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: name,
		Help: help,
	})
	b.R.MustRegister(proxyPushTime)
	proxyPushTime.SetToCurrentTime()

	err := b.P.Add()
	return err
}

func (b BasicPromPush) PushCountErrors(numErrors int) error {
	help := "The number of errors in the last round of proxy pushes"

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
