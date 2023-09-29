package amqclient

import (
	"context"
	"crypto/tls"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
	"time"
)

/*
 * Copyright (c) 2023 Trond Kandal, Norwegian University of Science and Technology
 */

// Consumer struct
type Consumer struct {
	amqpURI        string
	tls            *tls.Config
	exchange       string
	exchangeType   string
	queue          string
	routingKey     string
	ctag           string
	logger         *zap.SugaredLogger
	client         *amqpClient
	clientChanChan chan chan *amqpClient
	sendChan       chan amqp.Delivery
	cancel         context.CancelFunc
}

// NewConsumer allocates a new AMQP consumer, return a struct of itself, a delivery channel, or an error if
// something fails.
// Once this function is called it will reconnect to RabbitMQ endlessly until Shutdown is called.
func NewConsumer(amqpURI string, tls *tls.Config, exchange string, exchangeType string, queue string, key string,
	ctag string, logger *zap.SugaredLogger) (*Consumer, chan amqp.Delivery, error) {

	c := &Consumer{
		amqpURI:      amqpURI,
		tls:          tls,
		exchange:     exchange,
		exchangeType: exchangeType,
		queue:        queue,
		routingKey:   key,
		ctag:         ctag,
		logger:       logger,
		sendChan:     make(chan amqp.Delivery),
	}

	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel
	c.clientChanChan = c.redial(ctx)

	go c.handleDeliveries(ctx)
	return c, c.sendChan, nil
}

// Shutdown shuts down this consumer
func (c *Consumer) Shutdown() {
	c.logger.Warn("consumer received shutdown ...")
	c.cancel()
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
					c.logger.Errorf("cannot get a new client-channel; channel is closed")
					return
				}
				select {
				case c.client, ok = <-clientChan:
					if !ok {
						c.logger.Errorf("cannot get a new client; channel is closed")
						return
					}
				case <-ctx.Done():
					c.logger.Warnf("context done; error = %v", ctx.Err())
					return
				}
			case <-ctx.Done():
				c.logger.Warnf("context done; error = %v", ctx.Err())
				return
			}
			c.logger.Debugf("queue bound to exchange, starting consume (consumer tag '%s')", c.ctag)
			deliveries, err = c.client.channel.Consume(
				c.queue, // name
				c.ctag,  // consumerTag,
				true,    // autoAck
				false,   // exclusive
				false,   // noLocal
				false,   // noWait
				nil,     // arguments
			)
			if err != nil {
				c.logger.Errorf("deliver channel failed; error = %v", err)
				_ = c.client.close()
				c.client = nil
				continue
			}
		}

		select {
		case d, ok := <-deliveries:
			if !ok {
				c.logger.Warn("deliveries channel closed")
				_ = c.client.close()
				c.client = nil
				continue
			}
			if err := d.Ack(false); err != nil {
				c.logger.Errorf("acknowledge failed; error = %v", err)
				_ = d.Nack(false, false)
				continue
			}
			c.sendChan <- d
		case <-ctx.Done():
			c.logger.Errorf("context done; error = %v", ctx.Err())
			if !canceled {
				if err = c.client.channel.Cancel(c.ctag, false); err != nil {
					c.logger.Errorf("cancel channel failed; error = %v", err)
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

	go func() {
		clientChan := make(chan *amqpClient)
		defer close(clientChan)
		defer close(clientChanChan)

		for {
			select {
			case clientChanChan <- clientChan:
			case <-ctx.Done():
				c.logger.Warnf("context done; error = %v", ctx.Err())
				return
			}

			var err error
			var ac *amqpClient
			var delay = time.Duration(0)

			for ac == nil {
				ac, err = c.connect()
				if err != nil {
					delay = calculateDelay(delay)
					c.logger.Warnf("waiting %s before reconnect", delay)
					time.Sleep(delay)
				}
			}

			select {
			case clientChan <- ac:
			case <-ctx.Done():
				c.logger.Warnf("context done; error = %v", ctx.Err())
				return
			}
		}
	}()

	return clientChanChan
}

// connect and set up a channel to RabbitMQ.
func (c *Consumer) connect() (*amqpClient, error) {

	c.logger.Debugf("connecting to %s", c.amqpURI)
	connection, err := amqp.DialConfig(c.amqpURI, defaultAMQPConfig(c.tls))
	if err != nil {
		c.logger.Errorf("dial %s failed; error = %v ", c.amqpURI, err)
		return nil, err
	}

	c.logger.Debug("getting channel")
	channel, err := connection.Channel()
	if err != nil {
		c.logger.Errorf("get channel failed; error = %v", err)
		return nil, err
	}

	c.logger.Debug("setting QoS")
	if err = channel.Qos(1, 0, false); err != nil {
		c.logger.Errorf("set QoS failed; error = %v", err)
		return nil, err
	}

	c.logger.Debugf("declaring exchange (%s)", c.exchange)
	if err = channel.ExchangeDeclare(
		c.exchange,     // name of the exchange
		c.exchangeType, // type
		true,           // durable
		false,          // delete when complete
		false,          // internal
		false,          // noWait
		nil,            // arguments
	); err != nil {
		c.logger.Errorf("declare exchange failed; error = %v", err)
		return nil, err
	}

	c.logger.Debugf("declaring queue (%s)", c.queue)
	state, err := channel.QueueDeclare(
		c.queue, // name of the queue
		true,    // durable
		false,   // delete when usused
		false,   // exclusive
		false,   // noWait
		nil,     // arguments
	)
	if err != nil {
		c.logger.Errorf("declare queue failed; error = %v", err)
		return nil, err
	}

	c.logger.Debugf("declared queue (%d messages, %d consumers), binding to exchange (key '%s')",
		state.Messages, state.Consumers, c.routingKey)
	if err = channel.QueueBind(
		c.queue,      // name of the queue
		c.routingKey, // routingKey
		c.exchange,   // sourceExchange
		false,        // noWait
		nil,          // arguments
	); err != nil {
		c.logger.Errorf("bind queue failed; error = %v", err)
		return nil, err
	}

	return &amqpClient{
		connection: connection,
		channel:    channel,
	}, nil
}
