package amqclient

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/streadway/amqp"
	"go.uber.org/zap"
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
	sent           int64
	acks           int64
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
		sent:         0,
		acks:         0,
	}

	ctx, cancel := context.WithCancel(context.Background())
	p.cancel = cancel
	p.clientChanChan = redialProducer(ctx, p)
	return p, nil
}

// Publish publishes a new message with the exchange name and routing-key
func (p *Producer) Publish(exchange string, routingKey string, body []byte) error {
	p.logger.Debugf("publishing %s (%d bytes)", body, len(body))

	for {
		if p.client == nil {
			clientChan, ok := <-p.clientChanChan
			if !ok {
				return fmt.Errorf("cannot get a new producer-channel; channel is closed")
			}
			p.client, ok = <-clientChan
			if !ok {
				return fmt.Errorf("cannot get a new producer; channel is closed")
			}
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
		p.sent++
		if p.client.confirms != nil {
			for p.acks < p.sent {
				select {
				case confirm, ok := <-p.client.confirms:
					if !ok {
						return fmt.Errorf("confirm channel closed")
					}
					if !confirm.Ack {
						continue
					}
					p.logger.Infof("confirmed %d", confirm.DeliveryTag)
					p.acks++
				}
			}
		}
		break
	}
	return nil
}

func (p *Producer) Shutdown() {
	p.logger.Warn("producer received shutdown ...")
	p.cancel()
}

func redialProducer(ctx context.Context, p *Producer) chan chan *client {
	clientChanChan := make(chan chan *client)

	go func() {
		clientChan := make(chan *client)
		defer close(clientChan)
		defer close(clientChanChan)

		for {
			select {
			case clientChanChan <- clientChan:
			case <-ctx.Done():
				p.logger.Warnf("context done; error = %v", ctx.Err())
				return
			}

			var err error
			c := &client{
				connection: nil,
				channel:    nil,
				confirms:   nil,
			}
			p.logger.Debugf("connecting to %s", p.amqpURI)

			c.connection, err = amqp.DialConfig(p.amqpURI, defaultAMQPConfig(p.tls))
			if err != nil {
				p.logger.Errorf("dial %s failed; error = %v ", p.amqpURI, err)
				return
			}

			p.logger.Debug("getting Channel ")
			c.channel, err = c.connection.Channel()
			if err != nil {
				p.logger.Errorf("get channel failed; error = %v", err)
				return
			}

			p.logger.Debugf("declaring exchange (%s)", p.exchange)
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
				p.logger.Warnf("context done; error = %v", ctx.Err())
				return
			}
		}

	}()

	return clientChanChan
}
