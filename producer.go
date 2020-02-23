package amqclient

import (
	"context"
	"fmt"
	"github.com/streadway/amqp"
	"go.uber.org/zap"
)

// Producer struct
type Producer struct {
	amqpURI      string
	exchange     string
	exchangeType string
	key          string
	ctag         string
	reliable     bool
	logger       *zap.SugaredLogger
	client       *client
	clients      chan chan *client
	clientReady  bool
}

type client struct {
	connection *amqp.Connection
	channel    *amqp.Channel
	confirms   chan amqp.Confirmation
}

// NewProducer allocates a new amqp producer
func NewProducer(amqpURI string, exchange string, exchangeType string, key string, ctag string, reliable bool,
	logger *zap.SugaredLogger) (*Producer, error) {
	p := &Producer{
		amqpURI:      amqpURI,
		exchange:     exchange,
		exchangeType: exchangeType,
		key:          key,
		ctag:         ctag,
		reliable:     reliable,
		logger:       logger,
	}
	p.clients = redial(context.Background(), amqpURI, exchange, exchangeType, key, ctag, reliable, logger)
	return p, nil
	/*
		p := &Producer{
			amqpURI:    amqpURI,
			connection: nil,
			channel:    nil,
			confirms:   nil,
			reliable:   reliable,
			tag:        ctag,
			done:       make(chan error),
			logger:     logger,
		}

		var err error

		p.logger.Debugf("Connecting to %s", amqpURI)
		p.connection, err = amqp.Dial(amqpURI)
		if err != nil {
			return nil, fmt.Errorf("dial %s failed; error = %v ", amqpURI, err)
		}

		p.logger.Debug("Getting Channel ")
		p.channel, err = p.connection.Channel()
		if err != nil {
			return nil, fmt.Errorf("get channel failed; error = %v", err)
		}

		p.logger.Debugf("Declaring Exchange (%s)", exchange)
		if err := p.channel.ExchangeDeclare(
			exchange,     // name
			exchangeType, // type
			true,         // durable
			false,        // auto-deleted
			false,        // internal
			false,        // noWait
			nil,          // arguments
		); err != nil {
			return nil, fmt.Errorf("exchange declare failed; error = %v", err)
		}

		// Reliable publisher confirms require confirm.select support from the
		// connection.
		if p.reliable {
			if err := p.channel.Confirm(false); err != nil {
				return nil, fmt.Errorf("put channel in comfirm mode failed; error = %v", err)
			}

			p.confirms = p.channel.NotifyPublish(make(chan amqp.Confirmation, 1))
		}

		return p, nil
	*/
}

// Publish publishes a new message with the exchange name and routing-key
func (p *Producer) Publish(exchange string, routingKey string, body []byte) error {
	p.logger.Debugf("Publishing %s (%dB)", body, len(body))
	for {
		if p.client == nil {
			clients := <-p.clients
			p.client = <-clients
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
			if p.client.confirms != nil {
				close(p.client.confirms)
			}
			_ = p.client.channel.Close()
			_ = p.client.connection.Close()
			p.client = nil
			return fmt.Errorf("publish failed; error = %v", err)
		}
		if p.reliable {
			var confirm amqp.Confirmation
			var ok = true
			select {
			case confirm, ok = <-p.client.confirms:
				if !ok {
					return fmt.Errorf("confirm channel closed")
				}
				if !confirm.Ack {
					continue
				}
			}
			p.logger.Infof("confirmed %d", confirm.DeliveryTag)
		}
		break
	}
	return nil
	/*
		for {
			if err := p.channel.Publish(
				exchange,   // publish to an exchange
				routingKey, // routing to 0 or more queues
				false,      // mandatory
				false,      // immediate
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
				return fmt.Errorf("exchange publish failed; error = %v", err)
			}
			var confirm amqp.Confirmation
			var ok = true
			if p.reliable {
				select {
				case confirm, ok = <-p.confirms:
					if !ok {
						return fmt.Errorf("confirm channel closed")
					}
					if !confirm.Ack {
						continue
					}
				}
			}
			p.logger.Infof("confirmed %d", confirm.DeliveryTag)
			break
		}
		return nil
	*/
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

func redial(ctx context.Context, amqpURI string, exchange string, exchangeType string, key string, ctag string, reliable bool,
	logger *zap.SugaredLogger) chan chan *client {
	producers := make(chan chan *client)

	go func() {
		for {
			producer := make(chan *client)
			defer close(producers)

			for {
				select {
				case producers <- producer:
				case <-ctx.Done():
					logger.Errorf("context done; error = %v", ctx.Done())
					return
				}

				var err error
				c := &client{
					connection: nil,
					channel:    nil,
					confirms:   nil,
				}
				logger.Debugf("Connecting to %s", amqpURI)
				c.connection, err = amqp.Dial(amqpURI)
				if err != nil {
					logger.Errorf("dial %s failed; error = %v ", amqpURI, err)
					return
				}

				logger.Debug("Getting Channel ")
				c.channel, err = c.connection.Channel()
				if err != nil {
					logger.Errorf("get channel failed; error = %v", err)
					return
				}

				logger.Debugf("Declaring Exchange (%s)", exchange)
				if err := c.channel.ExchangeDeclare(
					exchange,     // name
					exchangeType, // type
					true,         // durable
					false,        // auto-deleted
					false,        // internal
					false,        // noWait
					nil,          // arguments
				); err != nil {
					logger.Errorf("exchange declare failed; error = %v", err)
					return
				}

				// Reliable publisher confirms require confirm.select support from the
				// connection.
				if reliable {
					if err := c.channel.Confirm(false); err != nil {
						logger.Errorf("put channel in comfirm mode failed; error = %v", err)
						return
					}
					c.confirms = c.channel.NotifyPublish(make(chan amqp.Confirmation, 1))
				}
				select {
				case producer <- c:
				case <-ctx.Done():
					logger.Errorf("context done; error = %v", ctx.Err())
					return
				}
			}
		}
	}()

	return producers
}
