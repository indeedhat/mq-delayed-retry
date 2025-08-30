package main

import (
	"context"
	"errors"
	"net/url"

	amqp "github.com/rabbitmq/amqp091-go"
)

func connect() (*amqp.Connection, *amqp.Channel, *amqp.Channel, error) {
	u, err := url.Parse(MqHost.Get() + ":" + MqPort.Get())
	if err != nil {
		return nil, nil, nil, err
	}

	u.User = url.UserPassword(MqUser.Get(), MqPass.Get())

	conn, err := amqp.Dial(u.String())
	if err != nil {
		return nil, nil, nil, err
	}

	subCh, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, nil, nil, err
	}
	pubCh, err := conn.Channel()
	if err != nil {
		conn.Close()
		subCh.Close()
		return nil, nil, nil, err
	}

	return conn, subCh, pubCh, nil
}

func createExchange(ch *amqp.Channel, name string) error {
	return ch.ExchangeDeclare(
		name,    // name
		"topic", // type
		true,    // durable
		false,   // auto-deleted
		false,   // internal
		false,   // no-wait
		nil,     // arguments
	)
}

func createQueue(ch *amqp.Channel, name string, headers ...amqp.Table) (amqp.Queue, error) {
	var headerTable amqp.Table
	if len(headers) > 0 {
		headerTable = headers[0]
	}

	return ch.QueueDeclare(
		name,        // name
		false,       // durable
		false,       // delete when unused
		false,       // exclusive
		false,       // no-wait
		headerTable, // arguments
	)
}

func bindQueue(ch *amqp.Channel, q amqp.Queue, exchange string, topics ...string) error {
	for _, topic := range topics {
		err := ch.QueueBind(
			q.Name,
			topic,
			exchange,
			false,
			nil,
		)
		if err != nil {
			return err
		}
	}

	return nil
}

func bindExchanges(ch *amqp.Channel, src, dst, topic string) error {
	return ch.ExchangeBind(dst, topic, src, false, nil)
}

func publishEvent(ctx context.Context, ch *amqp.Channel, exchange, topic string, msg any) error {
	var pub amqp.Publishing
	switch v := msg.(type) {
	case amqp.Publishing:
		pub = v
	case string:
		pub = amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(v),
		}
	default:
		return errors.New("unexpected message type")
	}
	return ch.PublishWithContext(
		ctx,
		exchange,
		topic,
		false,
		false,
		pub,
	)
}

func createConsumer(ctx context.Context, ch *amqp.Channel, qName, svcName string) (<-chan amqp.Delivery, error) {
	if err := ch.Qos(1, 0, true); err != nil {
		return nil, err
	}

	return ch.ConsumeWithContext(
		ctx,
		qName,   // queue
		svcName, // consumer
		false,   // auto ack
		false,   // exclusive
		false,   // no local
		false,   // no wait
		nil,     // args
	)
}
