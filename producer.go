package amqclient

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/streadway/amqp"
	"go.uber.org/zap"
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
	client         *amqpClient
	clientChanChan chan chan *amqpClient
	cancel         context.CancelFunc
	sent           int64
	acks           int64
	nacks          int64
}

// NewProducer allocates a new AMQP producer
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

// Publish publishes a new message with the exchange name and routing-key
func (p *Producer) Publish(exchange string, routingKey string, body []byte) error {
	p.logger.Debugf("publishing %s (%d bytes)", body, len(body))

	for {
		if p.client == nil {
			clientChan, ok := <-p.clientChanChan
			if !ok {
				return fmt.Errorf("cannot get a new producer-channel; error = channel is closed")
			}
			p.client, ok = <-clientChan
			if !ok {
				return fmt.Errorf("cannot get a new producer; error = channel is closed")
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
			if err := p.confirm(); err != nil {
				// Ignore error, only log
				p.logger.Warnf("get confirm failed; error = %v", err)
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
		}
	}
	return nil
}

// Shutdown shuts down this producer
func (p *Producer) Shutdown() {
	p.logger.Warn("producer received shutdown ...")
	if p.client.confirms != nil {
		if err := p.confirm(); err != nil {
			// Ignore error, only log
			p.logger.Warnf("get confirm failed; error = %v", err)
		}
	}
	p.cancel()
}

func (p *Producer) redial(ctx context.Context) chan chan *amqpClient {
	clientChanChan := make(chan chan *amqpClient)

	go func() {
		clientChan := make(chan *amqpClient)
		defer close(clientChan)
		defer close(clientChanChan)

		for {
			select {
			case clientChanChan <- clientChan:
			case <-ctx.Done():
				p.logger.Warnf("context done; error = %v", ctx.Err())
				_ = p.client.close()
				p.client = nil
				return
			}
			/*
				var err error
				var ac = &amqpclient{
					connection: nil,
					channel:    nil,
					confirms:   nil,
				}

				p.logger.Debugf("connecting to %s", p.amqpURI)
				delay := time.Duration(0)
				for {
					ac.connection, err = amqp.DialConfig(p.amqpURI, defaultAMQPConfig(p.tls))
					if err != nil {
						p.logger.Errorf("dial %s failed; error = %v ", p.amqpURI, err)
						delay = sleepDelay(delay)
						time.Sleep(delay)
						continue
					}
					break
				}

				p.logger.Debug("getting channel ")
				ac.channel, err = ac.connection.Channel()
				if err != nil {
					p.logger.Errorf("get channel failed; error = %v", err)
					return
				}

				p.logger.Debugf("declaring exchange (%s)", p.exchange)
				if err := ac.channel.ExchangeDeclare(
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
					if err := ac.channel.Confirm(false); err != nil {
						p.logger.Errorf("put channel in confirm mode failed; error = %v", err)
						return
					}
					ac.confirms = ac.channel.NotifyPublish(make(chan amqp.Confirmation, confirmCapacity))
				}
			*/
			var err error
			var ac *amqpClient
			var delay = time.Duration(0)

			for ac == nil {
				ac, err = p.connect()
				if err != nil {
					delay = connectDelay(delay)
					time.Sleep(delay)
					continue
				}
			}

			select {
			case clientChan <- ac:
			case <-ctx.Done():
				p.logger.Warnf("context done; error = %v", ctx.Err())
				_ = p.client.close()
				p.client = nil
				return
			}
		}

	}()

	return clientChanChan
}

func (p *Producer) connect() (*amqpClient, error) {

	p.logger.Debugf("connecting to %s", p.amqpURI)
	connection, err := amqp.DialConfig(p.amqpURI, defaultAMQPConfig(p.tls))
	if err != nil {
		p.logger.Errorf("dial %s failed; error = %v ", p.amqpURI, err)
		return nil, err
	}

	p.logger.Debug("getting channel ")
	channel, err := connection.Channel()
	if err != nil {
		p.logger.Errorf("get channel failed; error = %v", err)
		return nil, err
	}

	p.logger.Debugf("declaring exchange (%s)", p.exchange)
	if err := channel.ExchangeDeclare(
		p.exchange,     // name
		p.exchangeType, // type
		true,           // durable
		false,          // auto-deleted
		false,          // internal
		false,          // noWait
		nil,            // arguments
	); err != nil {
		p.logger.Errorf("exchange declare failed; error = %v", err)
		return nil, err
	}

	// Reliable publisher confirms require confirm.select support from the
	// connection.
	var confirms chan amqp.Confirmation
	if p.reliable {
		if err := channel.Confirm(false); err != nil {
			p.logger.Errorf("put channel in confirm mode failed; error = %v", err)
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
