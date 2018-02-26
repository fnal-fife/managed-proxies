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
		Help:      "The amount of time it took to run a stage (init|main|cleanup) of the Managed Proxies Service script",
	},
		[]string{
			"stage",
		},
	)

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
	if err := b.R.Register(promDuration); err != nil {
		return errors.New("Could not register promTotalDuration metric for monitoring")
	}

	if err := b.R.Register(proxyPushTime); err != nil {
		return errors.New("Could not register proxyPushTime metric for monitoring")
	}
	return nil
}

func (b BasicPromPush) PushNodeRoleTimestamp(experiment, node, role string) error {
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

func (b BasicPromPush) PushPromDuration(start time.Time, stage string) error {
	promDuration.WithLabelValues(stage).Set(time.Since(start).Seconds())
	err := b.P.Add()
	return err
}
