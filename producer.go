package amqclient

import (
	"context"
	"crypto/tls"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
	"time"
)

/*
 * Copyright (c) 2023 Trond Kandal, Norwegian University of Science and Technology
 */

const (
	mimeTextPlain   = "text/plain"
	contentEncoding = ""
	mesgPriority    = 0 // 0-9
	waitForConfirm  = 200 * time.Millisecond
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
	client         *amqpClient
	clientChanChan chan chan *amqpClient
	cancel         context.CancelFunc
	sent           int64
	acks           int64
	nacks          int64
}

// NewProducer allocates a new AMQP producer, return a struct of itself, or an error if something fails.
// Once this function is called it will reconnect to RabbitMQ endlessly until Shutdown is called.
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
	p.clientChanChan = p.redial(ctx)
	return p, nil
}

// Publish publishes a new message with the context, exchange name and routing-key.
func (p *Producer) Publish(ctx context.Context, exchange string, routingKey string, body []byte) error {
	p.logger.Debugf("publishing %s (%d bytes)", body, len(body))

	for {
		if p.client == nil {
			clientChan, ok := <-p.clientChanChan
			if !ok {
				return fmt.Errorf("failed to get a new producer-channel, channel is closed")
			}
			p.client, ok = <-clientChan
			if !ok {
				return fmt.Errorf("failed to get a new producer, channel is closed")
			}
		}

		if err := p.client.channel.PublishWithContext(ctx, exchange, routingKey, false, false,
			amqp.Publishing{
				Headers:         amqp.Table{},
				ContentType:     mimeTextPlain,
				ContentEncoding: contentEncoding,
				Body:            body,
				DeliveryMode:    amqp.Persistent, // 1=non-persistent, 2=persistent
				Priority:        mesgPriority,
				// a bunch of application/implementation-specific fields
			}); err != nil {

			p.logger.Errorw("failed to publish message", "error", err)
			_ = p.client.close()
			p.client = nil
			return err
		}
		p.sent++
		if p.client.confirms != nil {
			if err := p.confirm(); err != nil {
				// Ignore error, only log.
				p.logger.Warnw("failed to confirm message", "error", err)
			}
		}
		break
	}
	return nil
}

func (p *Producer) confirm() error {
	for p.acks < p.sent && !p.client.connection.IsClosed() {
		select {
		case confirm, ok := <-p.client.confirms:
			if !ok {
				return fmt.Errorf("confirm channel closed")
			}
			if !confirm.Ack {
				p.nacks++
				continue
			}
			p.acks++
			p.logger.Infof("confirmed %d", confirm.DeliveryTag)
			p.logger.Infof("confirmations (%d/%d/%d)", p.nacks, p.acks, p.sent)
		case <-time.After(waitForConfirm):
			return fmt.Errorf("no confirm(s) received after %s", waitForConfirm)
		}
	}
	return nil
}

// Shutdown shuts down this producer.
func (p *Producer) Shutdown() {
	p.logger.Warn("producer received shutdown ...")
	if p.client.confirms != nil {
		if err := p.confirm(); err != nil {
			// Ignore error, only log
			p.logger.Warnw("failed to confirm massages", "error", err)
		}
	}
	p.cancel()
}

// redial will connect to RabbitMQ endlessly, until Shutdown is called.
func (p *Producer) redial(ctx context.Context) chan chan *amqpClient {
	clientChanChan := make(chan chan *amqpClient)
	clientChan := make(chan *amqpClient)

	var err error
	var ac *amqpClient

	go func() {
		defer close(clientChan)
		defer close(clientChanChan)

		for {
			select {
			case clientChanChan <- clientChan:
			case <-ctx.Done():
				p.logger.Warnw("context canceled", "error", ctx.Err())
				_ = p.client.close()
				p.client = nil
				return
			}
			if isClosed(ac) {
				ac = nil
			}

			var delay = time.Duration(0)

			for ac == nil {
				ac, err = p.connect()
				if err != nil {
					delay = calculateDelay(delay)
					p.logger.Warnf("waiting %s before reconnect", delay)
					time.Sleep(delay)
				}
			}

			select {
			case clientChan <- ac:
			case <-ctx.Done():
				p.logger.Warnw("context canceled", "error", ctx.Err())
				_ = p.client.close()
				p.client = nil
				return
			}
		}

	}()

	return clientChanChan
}

// connect and set up a channel to RabbitMQ.
func (p *Producer) connect() (*amqpClient, error) {

	p.logger.Debugf("connecting to %s", p.amqpURI)
	connection, err := amqp.DialConfig(p.amqpURI, defaultAMQPConfig(p.tls))
	if err != nil {
		p.logger.Errorw(fmt.Sprintf("failed to dial %s", p.amqpURI), "error", err)
		return nil, err
	}

	p.logger.Debug("getting channel ")
	channel, err := connection.Channel()
	if err != nil {
		p.logger.Errorw("failed to get channel", "error", err)
		return nil, err
	}

	p.logger.Debugf("declaring exchange (%s)", p.exchange)
	if err = channel.ExchangeDeclare(
		p.exchange,     // name
		p.exchangeType, // type
		true,           // durable
		false,          // auto-deleted
		false,          // internal
		false,          // noWait
		nil,            // arguments
	); err != nil {
		p.logger.Errorw("failed to declare exchange", "error", err)
		return nil, err
	}

	// Reliable publisher confirms require confirm.select support from the
	// connection.
	var confirms chan amqp.Confirmation
	if p.reliable {
		if err := channel.Confirm(false); err != nil {
			p.logger.Errorw("failed to put put channel in confirm mode", "error", err)
			return nil, err
		}
		confirms = channel.NotifyPublish(make(chan amqp.Confirmation, confirmCapacity))
	}

	return &amqpClient{
		connection: connection,
		channel:    channel,
		confirms:   confirms,
	}, nil
}
