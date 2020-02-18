package amqclients

import (
	"fmt"
	"github.com/streadway/amqp"
	"go.uber.org/zap"
)

type Producer struct {
	connection *amqp.Connection
	channel    *amqp.Channel
	tag        string
	done       chan error
	logger     *zap.SugaredLogger
}

// NewProducer allocates a new amqp producer
func NewProducer(amqpURI string, exchange string, exchangeType string, key string, ctag string, reliable bool,
	logger *zap.SugaredLogger) (*Producer, error) {
	p := &Producer{
		connection: nil,
		channel:    nil,
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
	if reliable {
		if err := p.channel.Confirm(false); err != nil {
			return nil, fmt.Errorf("put channel in comnfirm mode failed; error = %v", err)
		}

		ack, nack := p.channel.NotifyConfirm(make(chan uint64, 1), make(chan uint64, 1))

		defer p.confirmOne(ack, nack)
	}

	return p, nil
}

// Publish publishes a new message with the exchange name and routing-key
func (p *Producer) Publish(exchange string, routingKey string, body []byte) error {
	p.logger.Debugf("Publishing %s (%dB)", body, len(body))

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

	return nil
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
