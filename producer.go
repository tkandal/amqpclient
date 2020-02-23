package amqclient

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/streadway/amqp"
	"go.uber.org/zap"
	"net"
	"time"
)

// Producer struct
type Producer struct {
	amqpURI        string
	tls            *tls.Config
	exchange       string
	exchangeType   string
	routingKey     string
	ctag           string
	reliable       bool
	logger         *zap.SugaredLogger
	client         *client
	clientChanChan chan chan *client
	cancel         context.CancelFunc
}

type client struct {
	connection *amqp.Connection
	channel    *amqp.Channel
	confirms   chan amqp.Confirmation
}

func (c *client) close() error {
	if c.confirms != nil {
		close(c.confirms)
	}
	if err := c.channel.Close(); err != nil {
		return err
	}
	return c.connection.Close()
}

// NewProducer allocates a new amqp producer
func NewProducer(amqpURI string, tls *tls.Config, exchange string, exchangeType string, key string, ctag string,
	reliable bool, logger *zap.SugaredLogger) (*Producer, error) {

	p := &Producer{
		amqpURI:      amqpURI,
		tls:          tls,
		exchange:     exchange,
		exchangeType: exchangeType,
		routingKey:   key,
		ctag:         ctag,
		reliable:     reliable,
		logger:       logger,
	}

	ctx, cancel := context.WithCancel(context.Background())
	p.cancel = cancel
	p.clientChanChan = redialProducer(ctx, p)
	return p, nil
}

// Publish publishes a new message with the exchange name and routing-key
func (p *Producer) Publish(exchange string, routingKey string, body []byte) error {
	p.logger.Debugf("Publishing %s (%dB)", body, len(body))

	for {
		if p.client == nil {
			clientChan, ok := <-p.clientChanChan
			if !ok {
				return fmt.Errorf("cannot get a new producer; channel is closed")
			}
			p.client = <-clientChan
		}

		if err := p.client.channel.Publish(
			exchange,
			routingKey,
			false,
			false,
			amqp.Publishing{
				Headers:         amqp.Table{},
				ContentType:     "text/plain",
				ContentEncoding: "",
				Body:            body,
				DeliveryMode:    amqp.Transient, // 1=non-persistent, 2=persistent
				Priority:        0,              // 0-9
				// a bunch of application/implementation-specific fields
			},
		); err != nil {
			p.logger.Errorf("publish failed; error = %v", err)
			_ = p.client.close()
			p.client = nil
			return err
		}
		if p.client.confirms != nil {
			select {
			case confirm, ok := <-p.client.confirms:
				if !ok {
					return fmt.Errorf("confirm channel closed")
				}
				if !confirm.Ack {
					continue
				}
				p.logger.Infof("confirmed %d", confirm.DeliveryTag)
			}
		}
		break
	}
	return nil
}

func (p *Producer) Shutdown() {
	p.cancel()
}

// One would typically keep a channel of publishings, a sequence number, and a
// set of unacknowledged sequence numbers and loop until the publishing channel
// is closed.
func (p *Producer) confirmOne(ack, nack chan uint64) {
	p.logger.Debug("waiting for confirmation of one publishing")

	select {
	case tag := <-ack:
		p.logger.Infof("confirmed delivery with delivery tag: %d", tag)
	case tag := <-nack:
		p.logger.Warnf("failed delivery of delivery tag: %d", tag)
	}
}

func redialProducer(ctx context.Context, p *Producer) chan chan *client {
	clientChanChan := make(chan chan *client)

	go func() {
		clientChan := make(chan *client)
		defer close(clientChanChan)
		defer close(clientChan)

		for {
			select {
			case clientChanChan <- clientChan:
			case <-ctx.Done():
				p.logger.Errorf("context done; error = %v", ctx.Done())
				return
			}

			var err error
			c := &client{
				connection: nil,
				channel:    nil,
				confirms:   nil,
			}
			p.logger.Debugf("Connecting to %s", p.amqpURI)
			cfg := amqp.Config{
				Heartbeat: 10 * time.Second,
				Dial: func(nw string, addr string) (net.Conn, error) {
					return net.DialTimeout(nw, addr, 10*time.Second)
				},
			}
			if p.tls != nil {
				cfg.TLSClientConfig = p.tls
			}

			c.connection, err = amqp.DialConfig(p.amqpURI, cfg)
			if err != nil {
				p.logger.Errorf("dial %s failed; error = %v ", p.amqpURI, err)
				return
			}

			p.logger.Debug("Getting Channel ")
			c.channel, err = c.connection.Channel()
			if err != nil {
				p.logger.Errorf("get channel failed; error = %v", err)
				return
			}

			p.logger.Debugf("Declaring Exchange (%s)", p.exchange)
			if err := c.channel.ExchangeDeclare(
				p.exchange,     // name
				p.exchangeType, // type
				true,           // durable
				false,          // auto-deleted
				false,          // internal
				false,          // noWait
				nil,            // arguments
			); err != nil {
				p.logger.Errorf("exchange declare failed; error = %v", err)
				return
			}

			// Reliable publisher confirms require confirm.select support from the
			// connection.
			if p.reliable {
				if err := c.channel.Confirm(false); err != nil {
					p.logger.Errorf("put channel in confirm mode failed; error = %v", err)
					return
				}
				c.confirms = c.channel.NotifyPublish(make(chan amqp.Confirmation, 1))
			}
			select {
			case clientChan <- c:
			case <-ctx.Done():
				p.logger.Errorf("context done; error = %v", ctx.Err())
				return
			}
		}

	}()

	return clientChanChan
}
