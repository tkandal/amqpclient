package amqclient

import (
	"crypto/tls"
	"github.com/streadway/amqp"
	"net"
	"time"
)

const (
	defaultDialTimeout = 10 * time.Second
	defaultHeartbeat   = 10 * time.Second
)

func defaultAMQPConfig(tls *tls.Config) amqp.Config {
	cfg := amqp.Config{
		Heartbeat: defaultHeartbeat,
		Dial:      amqpDialer,
	}
	if tls != nil {
		cfg.TLSClientConfig = tls
	}
	return cfg
}

func amqpDialer(nw string, addr string) (net.Conn, error) {
	return net.DialTimeout(nw, addr, defaultDialTimeout)
}
