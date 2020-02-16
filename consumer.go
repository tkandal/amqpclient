package rmqclients

import (
	"fmt"
	"github.com/streadway/amqp"
	"go.uber.org/zap"
)

type Consumer struct {
	amqpURI    string
	connection *amqp.Connection
	channel    *amqp.Channel
	tag        string
	done       chan error
	logger     *zap.SugaredLogger
	quit       chan bool
}

func NewConsumer(amqpURI string, exchange string, exchangeType string, queue string, key string, ctag string,
	logger *zap.SugaredLogger) (*Consumer, chan amqp.Delivery, error) {
	c := &Consumer{
		amqpURI:    amqpURI,
		connection: nil,
		channel:    nil,
		tag:        ctag,
		done:       make(chan error),
		logger:     logger,
		quit:       make(chan bool),
	}

	var err error

	c.logger.Debug("Connecting to %s", amqpURI)
	c.connection, err = amqp.Dial(amqpURI)
	if err != nil {
		return nil, nil, fmt.Errorf("dial %s failed; error = %v", amqpURI, err)
	}

	c.logger.Debug("Getting Channel")
	c.channel, err = c.connection.Channel()
	if err != nil {
		return nil, nil, fmt.Errorf("get channel failed; error = %v", err)
	}

	c.logger.Debugf("Declaring Exchange (%s)", exchange)
	if err = c.channel.ExchangeDeclare(
		exchange,     // name of the exchange
		exchangeType, // type
		true,         // durable
		false,        // delete when complete
		false,        // internal
		false,        // noWait
		nil,          // arguments
	); err != nil {
		return nil, nil, fmt.Errorf("exchange declare failed; error = %v", err)
	}

	c.logger.Debugf("Declaring Queue (%s)", queue)
	state, err := c.channel.QueueDeclare(
		queue, // name of the queue
		true,  // durable
		false, // delete when usused
		false, // exclusive
		false, // noWait
		nil,   // arguments
	)
	if err != nil {
		return nil, nil, fmt.Errorf("queue declare failed; error = %v", err)
	}

	c.logger.Debugf("Declared Queue (%d messages, %d consumers), binding to Exchange (key '%s')",
		state.Messages, state.Consumers, key)
	if err = c.channel.QueueBind(
		queue,    // name of the queue
		key,      // routingKey
		exchange, // sourceExchange
		false,    // noWait
		nil,      // arguments
	); err != nil {
		return nil, nil, fmt.Errorf("queue bind failed; error = %v", err)
	}

	c.logger.Debugf("queue bound to exchange, starting consume (consumer tag '%s')", c.tag)
	deliveries, err := c.channel.Consume(
		queue, // name
		c.tag, // consumerTag,
		true,  // autoAck
		false, // exclusive
		false, // noLocal
		false, // noWait
		nil,   // arguments
	)
	if err != nil {
		return nil, nil, fmt.Errorf("queue consume failed; error = %v", err)
	}

	sendQ := make(chan amqp.Delivery)

	go c.handle(deliveries, sendQ, c.quit)

	return c, sendQ, nil
}

func (c *Consumer) Shutdown() error {
	// will close() the deliveries channel
	if err := c.channel.Cancel(c.tag, true); err != nil {
		return fmt.Errorf("consumer cancel failed; error = %v", err)
	}

	if err := c.connection.Close(); err != nil {
		return fmt.Errorf("AMQP connection close failed; error = %v", err)
	}

	defer c.logger.Debug("AMQP shutdown OK")
	c.quit <- true
	// wait for handle() to exit
	close(c.quit)
	return <-c.done
}

func (c *Consumer) handle(deliveries <-chan amqp.Delivery, sendQ chan amqp.Delivery, quit chan bool) {
	/*
		for d := range deliveries {
			c.logger.Infof(
				"Got %dB delivery: [%v] %s",
				len(d.Body),
				d.DeliveryTag,
				d.Body,
			)
			if err := d.Ack(false); err != nil {
				c.done <- err
			}
		}
	*/
	var d amqp.Delivery
	var ok = true
	for {
		select {
		case d, ok = <-deliveries:
			if ok {
				if err := d.Ack(false); err != nil {
					c.done <- err
				}
				sendQ <- d
			}
		case <-quit:
			ok = false
		}
		if !ok {
			break
		}
	}
	c.logger.Warn("Handle: deliveries channel closed")
	close(sendQ)
	c.done <- nil
}
