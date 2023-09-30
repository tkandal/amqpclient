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

// Consumer struct
type Consumer struct {
	commonClient
	log            *zap.SugaredLogger
	client         *amqpClient
	clientChanChan chan chan *amqpClient
	sendChan       chan amqp.Delivery
	cancel         context.CancelFunc
}

// NewConsumer allocates a new AMQP consumer, return a struct of itself, a delivery channel, or an error if
// something fails.
// Once this function is called it will reconnect to RabbitMQ endlessly until Shutdown is called.
func NewConsumer(cfg *AMQPConfig, log *zap.SugaredLogger, opts ...AMQPOption) (*Consumer, chan amqp.Delivery, error) {
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

	c := &Consumer{
		commonClient: commonClient{cfg: conf},
		log:          log,
	}
	for _, opt := range opts {
		opt(c.cfg)
	}
	c.sendChan = make(chan amqp.Delivery, c.cfg.AMQPPrefetch)

	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel
	c.clientChanChan = c.redial(ctx)

	go c.handleDeliveries(ctx)
	return c, c.sendChan, nil
}

// Shutdown shuts down this consumer
func (c *Consumer) Shutdown() {
	c.log.Warn("consumer received shutdown ...")
	c.cancel()
}

// Config returns the current configuration.
func (c *Consumer) Config() *AMQPConfig {
	return c.cfg
}

func (c *Consumer) handleDeliveries(ctx context.Context) {
	defer close(c.sendChan)

	var deliveries <-chan amqp.Delivery
	var err error
	var canceled = false
	for {
		if c.client == nil {
			select {
			case clientChan, ok := <-c.clientChanChan:
				if !ok {
					c.log.Errorf("cannot get a new client-channel, channel is closed")
					return
				}
				select {
				case c.client, ok = <-clientChan:
					if !ok {
						c.log.Errorf("cannot get a new client, channel is closed")
						return
					}
				case <-ctx.Done():
					c.log.Warnw("context canceled", "error", ctx.Err())
					return
				}
			case <-ctx.Done():
				c.log.Warnw("context canceled", "error", ctx.Err())
				return
			}
			c.log.Debugf("queue bound to exchange, starting consume (consumer tag '%s')", c.cfg.ClientName)
			deliveries, err = c.client.channel.Consume(
				c.cfg.AMQPQueue,  // name
				c.cfg.ClientName, // consumerTag,
				false,            // autoAck
				false,            // exclusive
				false,            // noLocal
				false,            // noWait
				nil,              // arguments
			)
			if err != nil {
				c.log.Errorw("failed to get deliver channel", "error", err)
				_ = c.client.close()
				c.client = nil
				continue
			}
		}

		select {
		case d, ok := <-deliveries:
			if !ok {
				c.log.Warn("deliveries channel closed")
				_ = c.client.close()
				c.client = nil
				continue
			}
			c.sendChan <- d
			if err = d.Ack(false); err != nil {
				c.log.Warnw(fmt.Sprintf("failed to ack delivery %d", d.DeliveryTag), "error", err)
				if err = d.Nack(false, true); err != nil {
					c.log.Warnw("failed to nack delivery", "error", err)
				}
			}
		case <-ctx.Done():
			c.log.Errorw("context canceled", "error", ctx.Err())
			if !canceled {
				if err = c.client.channel.Cancel(c.cfg.ClientName, false); err != nil {
					c.log.Warnw("failed to cancel channel", "error", err)
					continue
				}
				canceled = true
			}
		}
	}
}

// redial will connect to RabbitMQ endlessly, until Shutdown is called.
func (c *Consumer) redial(ctx context.Context) chan chan *amqpClient {
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
				c.log.Warnw("context canceled", "error", ctx.Err())
				return
			}
			if isClosed(ac) {
				ac = nil
			}
			var delay = time.Duration(0)

			for ac == nil {
				ac, err = c.connect()
				if err != nil {
					delay = calculateDelay(delay)
					c.log.Warnf("waiting %s before reconnect", delay)
					time.Sleep(delay)
				}
			}

			select {
			case clientChan <- ac:
			case <-ctx.Done():
				c.log.Warnw("context canceled", "error", ctx.Err())
				return
			}
		}
	}()

	return clientChanChan
}

// connect and set up a channel to RabbitMQ.
func (c *Consumer) connect() (*amqpClient, error) {

	c.log.Debugf("connecting to %s", c.cfg.AMQPServer)
	uri, err := fullURI(c.cfg)
	if err != nil {
		c.log.Errorw("failed to parse server URL", "error", err)
		return nil, err
	}
	connection, err := amqp.DialConfig(uri, defaultAMQPConfig(c.cfg))
	if err != nil {
		c.log.Errorw(fmt.Sprintf("failed to dial %s", c.cfg.AMQPServer), "error", err)
		return nil, err
	}

	c.log.Debug("getting channel")
	channel, err := connection.Channel()
	if err != nil {
		c.log.Errorw("failed to get channel", "error", err)
		return nil, err
	}

	c.log.Debug("setting QoS")
	if err = channel.Qos(c.cfg.AMQPPrefetch, 0, false); err != nil {
		c.log.Errorw("failed to set QoS", "error", err)
		return nil, err
	}

	c.log.Debugf("declaring exchange (%s)", c.cfg.AMQPExchange)
	if err = channel.ExchangeDeclare(
		c.cfg.AMQPExchange,     // name of the exchange
		c.cfg.AMQPExchangeType, // type
		true,                   // durable
		false,                  // delete when complete
		false,                  // internal
		false,                  // noWait
		nil,                    // arguments
	); err != nil {
		c.log.Errorw("failed to declare exchange", "error", err)
		return nil, err
	}

	c.log.Debugf("declaring queue (%s)", c.cfg.AMQPQueue)
	state, err := channel.QueueDeclare(
		c.cfg.AMQPQueue, // name of the queue
		true,            // durable
		false,           // delete when unused
		false,           // exclusive
		false,           // noWait
		nil,             // arguments
	)
	if err != nil {
		c.log.Errorw("failed to declare queue", "error", err)
		return nil, err
	}

	c.log.Debugf("declared queue (%d messages, %d consumers), binding to exchange (key '%s')",
		state.Messages, state.Consumers, c.cfg.AMQPRoutingKey)
	if err = channel.QueueBind(
		c.cfg.AMQPQueue,      // name of the queue
		c.cfg.AMQPRoutingKey, // routingKey
		c.cfg.AMQPExchange,   // sourceExchange
		false,                // noWait
		nil,                  // arguments
	); err != nil {
		c.log.Errorw("failed to bind queue", "error", err)
		return nil, err
	}

	return &amqpClient{
		connection: connection,
		channel:    channel,
	}, nil
}
