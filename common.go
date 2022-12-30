package amqclient

import (
	"crypto/tls"
	"net"
	"time"

	"github.com/streadway/amqp"
)

const (
	defaultDialTimeout = 10 * time.Second
	defaultHeartbeat   = 10 * time.Second
	maxReconnectDelay  = 30 * time.Second
	confirmCapacity    = 128
)

type amqpClient struct {
	connection *amqp.Connection
	channel    *amqp.Channel
	confirms   chan amqp.Confirmation
}

func (ac *amqpClient) close() error {
	if ac.channel != nil {
		if err := ac.channel.Close(); err != nil {
			return err
		}
		ac.channel = nil
	}
	if ac.connection != nil {
		if err := ac.connection.Close(); err != nil {
			return err
		}
		ac.connection = nil
	}
	return nil
}

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

func calculateDelay(d time.Duration) time.Duration {
	d += time.Second
	if d > maxReconnectDelay {
		d = maxReconnectDelay
	}
	return d
}
