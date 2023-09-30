package amqclient

import (
	"context"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
	"os"
	"path/filepath"
	"time"
)

/*
 * Copyright (c) 2023 Trond Kandal, Norwegian University of Science and Technology
 */

// Producer struct.
type Producer struct {
	commonClient
	reliable       bool
	log            *zap.SugaredLogger
	client         *amqpClient
	clientChanChan chan chan *amqpClient
	cancel         context.CancelFunc
	sent           int64
	acks           int64
	nacks          int64
}

// NewProducer allocates a new AMQP-producer, return a struct of itself, or an error if something fails.
// Once this function is called it will reconnect to RabbitMQ endlessly until Shutdown is called.
func NewProducer(cfg *AMQPConfig, reliable bool, logger *zap.SugaredLogger, opts ...AMQPOption) (*Producer, error) {
	conf := cfg
	if conf == nil {
		conf = &AMQPConfig{
			AMQPServer:          defaultServer,
			AMQPUser:            "",
			AMQPPass:            "",
			AMQPVHost:           defaultVhost,
			AMQPExchange:        "",
			AMQPExchangeType:    amqp.ExchangeDirect,
			AMQPQueue:           "",
			AMQPPrefetch:        defaultPrefetch,
			AMQPRoutingKey:      "",
			AMQPTimeout:         defaultTimeout,
			AMQPContentType:     defaultContentType,
			AMQPContentEncoding: defaultEncoding,
			TLS:                 nil,
			ClientName:          filepath.Base(os.Args[0]),
		}
	}

	p := &Producer{
		commonClient: commonClient{cfg: conf},
		reliable:     reliable,
		log:          logger,
	}
	for _, opt := range opts {
		opt(p.cfg)
	}

	ctx, cancel := context.WithCancel(context.Background())
	p.cancel = cancel
	p.clientChanChan = p.redial(ctx)
	return p, nil
}

// Publish publishes a new message with the context, exchange name and routing-key.
func (p *Producer) Publish(ctx context.Context, body []byte) error {
	p.log.Debugf("publishing %s (%d bytes)", body, len(body))

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

		if err := p.client.channel.PublishWithContext(ctx, p.cfg.AMQPExchange, p.cfg.AMQPRoutingKey, false, false,
			amqp.Publishing{
				Headers:         amqp.Table{},
				ContentType:     p.cfg.AMQPContentType,
				ContentEncoding: p.cfg.AMQPContentEncoding,
				DeliveryMode:    amqp.Persistent, // 1=non-persistent, 2=persistent
				Priority:        mesgPriority,
				Timestamp:       time.Now(),
				Body:            body,
				// a bunch of application/implementation-specific fields
			}); err != nil {

			p.log.Errorw("failed to publish message", "error", err)
			_ = p.client.close()
			p.client = nil
			return err
		}
		p.sent++
		if p.client.confirms != nil {
			if err := p.confirm(); err != nil {
				// Ignore error, only log.
				p.log.Warnw("failed to confirm message", "error", err)
			}
		}
		break
	}
	return nil
}

func (p *Producer) confirm() error {
	for p.acks < p.sent && !isClosed(p.client) {
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
			p.log.Infof("confirmed %d", confirm.DeliveryTag)
			p.log.Infof("confirmations (%d/%d/%d)", p.nacks, p.acks, p.sent)
		case <-time.After(waitForConfirm):
			return fmt.Errorf("no confirm(s) received after %s", waitForConfirm)
		}
	}
	return nil
}

// Shutdown shuts down this producer.
func (p *Producer) Shutdown() {
	p.log.Warn("producer received shutdown ...")
	if p.client.confirms != nil {
		if err := p.confirm(); err != nil {
			// Ignore error, only log
			p.log.Warnw("failed to confirm massages", "error", err)
		}
	}
	p.cancel()
}

// Config returns the current configuration.
func (p *Producer) Config() *AMQPConfig {
	return p.cfg
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
				p.log.Warnw("context canceled", "error", ctx.Err())
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
					p.log.Warnf("waiting %s before reconnect", delay)
					time.Sleep(delay)
				}
			}

			select {
			case clientChan <- ac:
			case <-ctx.Done():
				p.log.Warnw("context canceled", "error", ctx.Err())
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

	p.log.Debugf("connecting to %s", p.cfg.AMQPServer)
	uri, err := fullURI(p.cfg)
	if err != nil {
		p.log.Errorw("failed to parse server URL", "error", err)
		return nil, err
	}
	connection, err := amqp.DialConfig(uri, defaultAMQPConfig(p.cfg))
	if err != nil {
		p.log.Errorw(fmt.Sprintf("failed to dial %s", p.cfg.AMQPServer), "error", err)
		return nil, err
	}

	p.log.Debug("getting channel ")
	channel, err := connection.Channel()
	if err != nil {
		p.log.Errorw("failed to get channel", "error", err)
		return nil, err
	}

	p.log.Debugf("declaring exchange (%s)", p.cfg.AMQPExchange)
	if err = channel.ExchangeDeclare(
		p.cfg.AMQPExchange,     // name
		p.cfg.AMQPExchangeType, // type
		true,                   // durable
		false,                  // auto-deleted
		false,                  // internal
		false,                  // noWait
		nil,                    // arguments
	); err != nil {
		p.log.Errorw("failed to declare exchange", "error", err)
		return nil, err
	}

	p.log.Debugf("declaring queue (%s)", p.cfg.AMQPQueue)
	state, err := channel.QueueDeclare(
		p.cfg.AMQPQueue, // name of the queue
		true,            // durable
		false,           // delete when unused
		false,           // exclusive
		false,           // noWait
		nil,             // arguments
	)
	if err != nil {
		p.log.Errorw("failed to declare queue", "error", err)
		return nil, err
	}
	p.log.Debugf("declared queue (%d messages, %d consumers), binding to exchange (key '%s')",
		state.Messages, state.Consumers, p.cfg.AMQPRoutingKey)

	if err = channel.QueueBind(
		p.cfg.AMQPQueue,      // name of the queue
		p.cfg.AMQPRoutingKey, // routingKey
		p.cfg.AMQPExchange,   // sourceExchange
		false,                // noWait
		nil,                  // arguments
	); err != nil {
		p.log.Errorw("failed to bind queue", "error", err)
		return nil, err
	}

	// Reliable publisher confirms require confirm.select support from the
	// connection.
	var confirms chan amqp.Confirmation
	if p.reliable {
		if err := channel.Confirm(false); err != nil {
			p.log.Errorw("failed to put put channel in confirm mode", "error", err)
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
