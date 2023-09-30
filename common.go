package amqclient

import (
	"crypto/tls"
	amqp "github.com/rabbitmq/amqp091-go"
	"net"
	"net/url"
	"time"
)

/*
 * Copyright (c) 2023 Trond Kandal, Norwegian University of Science and Technology
 */

const (
	defaultServer      = "amqp://localhost:5672"
	defaultVhost       = "/"
	defaultMaxChannels = 16
	defaultPrefetch    = 1
	defaultTimeout     = 10 * time.Second
	defaultContentType = "text/plain"
	defaultHeartbeat   = 10 * time.Second
	defaultLocale      = "en_US"
	maxReconnectDelay  = 30 * time.Second
	confirmCapacity    = 128
	defaultEncoding    = ""
)

// AMQPConfig is the configuration for both consumer and producer.
// One may either send in this object to the constructors as parameter or use the option functions as parameters.
type AMQPConfig struct {
	AMQPServer          string        `json:"AMQPServer"`
	AMQPUser            string        `json:"AMQPUser"`
	AMQPPass            string        `json:"-"`
	AMQPVHost           string        `json:"AMQPVHost"`
	AMQPExchange        string        `json:"AMQPExchange"`
	AMQPExchangeType    string        `json:"AMQPExchangeType"`
	AMQPQueue           string        `json:"AMQPQueue"`
	AMQPPrefetch        int           `json:"AMQPPrefetch"`
	AMQPRoutingKey      string        `json:"AMQPRoutingKey"`
	AMQPTimeout         time.Duration `json:"AMQPTimeout"`
	AMQPContentType     string        `json:"AMQPContentType"`
	AMQPContentEncoding string        `json:"AMQPContentEncoding"`
	TLS                 *tls.Config   `json:"-"`
	ClientName          string        `json:"ClientName"`
}

// AMQPOption defines an option function that may be used as parameter to the constructors.
type AMQPOption func(c *AMQPConfig) AMQPOption

// AMQPServer set the URL for the AMQP-server, must have the following format amqp://host:port.
// Default amqp://localhost:5672
func AMQPServer(s string) AMQPOption {
	return func(c *AMQPConfig) AMQPOption {
		prev := c.AMQPServer
		c.AMQPServer = s
		return AMQPServer(prev)
	}
}

// AMQPUser set the username for connections to the AMQP-server.
func AMQPUser(s string) AMQPOption {
	return func(c *AMQPConfig) AMQPOption {
		prev := c.AMQPUser
		c.AMQPUser = s
		return AMQPUser(prev)
	}
}

// AMQPPass set the password for connections to the AMQP-server.
func AMQPPass(s string) AMQPOption {
	return func(c *AMQPConfig) AMQPOption {
		prev := c.AMQPPass
		c.AMQPPass = s
		return AMQPPass(prev)
	}
}

// AMQPVHost set the name for the virtual host on the AMQP-server. Default /.
func AMQPVHost(s string) AMQPOption {
	return func(c *AMQPConfig) AMQPOption {
		prev := c.AMQPVHost
		c.AMQPVHost = s
		return AMQPVHost(prev)
	}
}

// AMQPExchange set the name for the exchange on the AMQP-server.
func AMQPExchange(s string) AMQPOption {
	return func(c *AMQPConfig) AMQPOption {
		prev := c.AMQPExchange
		c.AMQPExchange = s
		return AMQPExchange(prev)
	}
}

// AMQPExchangeType set the type for the exchange on the AMQP-server. Default direct.
func AMQPExchangeType(s string) AMQPOption {
	return func(c *AMQPConfig) AMQPOption {
		prev := c.AMQPContentType
		c.AMQPExchangeType = s
		return AMQPExchangeType(prev)
	}
}

// AMQPQueue set the name of queue on the AMQP-server.
func AMQPQueue(s string) AMQPOption {
	return func(c *AMQPConfig) AMQPOption {
		prev := c.AMQPQueue
		c.AMQPQueue = s
		return AMQPQueue(prev)
	}
}

// AMQPPrefetch set the number of prefetch on the AMQP-server.  Default 1.
func AMQPPrefetch(p int) AMQPOption {
	return func(c *AMQPConfig) AMQPOption {
		prev := c.AMQPPrefetch
		c.AMQPPrefetch = p
		return AMQPPrefetch(prev)
	}
}

// AMQPRoutingKey set the name of the routing key on the AMQP-server.
func AMQPRoutingKey(s string) AMQPOption {
	return func(c *AMQPConfig) AMQPOption {
		prev := c.AMQPRoutingKey
		c.AMQPRoutingKey = s
		return AMQPRoutingKey(prev)
	}
}

// AMQPTimeout set the timeout for connecting to the AMQP-server. Default 10 seconds.
func AMQPTimeout(d time.Duration) AMQPOption {
	return func(c *AMQPConfig) AMQPOption {
		prev := c.AMQPTimeout
		c.AMQPTimeout = d
		return AMQPTimeout(prev)
	}
}

// AMQPContentType set the MIME content type for messages on the AMQP-server. Default text/plain.
func AMQPContentType(s string) AMQPOption {
	return func(c *AMQPConfig) AMQPOption {
		prev := c.AMQPContentType
		c.AMQPContentType = s
		return AMQPContentType(prev)
	}
}

// AMQPContentEncoding set the MIME encoding type for messages on the AMQP-server.
func AMQPContentEncoding(s string) AMQPOption {
	return func(c *AMQPConfig) AMQPOption {
		prev := c.AMQPContentEncoding
		c.AMQPContentEncoding = s
		return AMQPContentEncoding(prev)
	}
}

// TLS set the TLS config for connections to the AMQP-server.  Default nil.
func TLS(tls *tls.Config) AMQPOption {
	return func(c *AMQPConfig) AMQPOption {
		prev := c.TLS
		c.TLS = tls
		return TLS(prev)
	}
}

// ClientName set the client name for the connections to the AMQP-server.
func ClientName(s string) AMQPOption {
	return func(c *AMQPConfig) AMQPOption {
		prev := c.ClientName
		c.ClientName = s
		return ClientName(prev)
	}
}

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

func defaultAMQPConfig(config *AMQPConfig) amqp.Config {
	cfg := amqp.Config{
		Vhost:      config.AMQPVHost,
		ChannelMax: defaultMaxChannels,
		Heartbeat:  defaultHeartbeat,
		Dial:       amqpDialer(config.AMQPTimeout),
		Locale:     defaultLocale,
	}
	if config.TLS != nil {
		cfg.TLSClientConfig = config.TLS
	}
	return cfg
}

func amqpDialer(d time.Duration) func(string, string) (net.Conn, error) {
	return func(nw string, addr string) (net.Conn, error) {
		return net.DialTimeout(nw, addr, d)
	}
}

func calculateDelay(d time.Duration) time.Duration {
	if d < maxReconnectDelay {
		d += time.Second
	}
	return d
}

func isClosed(ac *amqpClient) bool {
	if ac == nil {
		return true
	}
	return ac != nil && ((ac.channel != nil && ac.channel.IsClosed()) || (ac.connection != nil && ac.connection.IsClosed()))
}

func fullURI(config *AMQPConfig) (string, error) {
	uri, err := url.Parse(config.AMQPServer)
	if err != nil {
		return "", err
	}
	if len(config.AMQPUser) > 0 || len(config.AMQPPass) > 0 {
		uri.User = url.UserPassword(config.AMQPUser, config.AMQPPass)
	}
	return uri.String(), nil
}
