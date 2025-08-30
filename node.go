package main

import (
	"context"
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	ExchangeDlx     = "dlx"
	ExchangePublish = "publish"
	ExchangeBackoff = "backoff"
	ExchangeRetry   = "retry"
)

var BackoffDurations = []time.Duration{
	1 * time.Second,
	5 * time.Second,
	10 * time.Second,
}

func buildTopology(ch *amqp.Channel, svcName string, topics []string) error {
	// dlx is the final exchange/queue messages will be sent to if all retry's fail
	if err := createExchange(ch, ExchangeDlx); err != nil {
		return err
	}
	eq, err := createQueue(ch, "errors")
	if err != nil {
		return err
	}
	if err := bindQueue(ch, eq, ExchangeDlx, "error"); err != nil {
		return err
	}

	// svc_[svcName] is an exchange/queue combo that is unique to the specific node/service, this is where a
	// node/service will bind their topics.
	//
	// We have a single exchange per node/service specifically so an event can be retried on a specific service
	// without having to republish it to any other node/service that might have already handled the event without error
	if err := createExchange(ch, "svc_"+svcName); err != nil {
		return err
	}
	sq, err := createQueue(ch, svcName, amqp.Table{
		"x-dead-letter-exchange":    ExchangeDlx,
		"x-dead-letter-routing-key": "error",
	})
	if err != nil {
		return err
	}
	if err := bindQueue(ch, sq, "svc_"+svcName, topics...); err != nil {
		return err
	}
	if err := bindQueue(ch, sq, "svc_"+svcName, "RETRY.#"); err != nil {
		return err
	}

	// publish is the exchange that all services will publish their events to.
	// any event published to this echange will be forwaded to all other service exchanges
	if err := createExchange(ch, ExchangePublish); err != nil {
		return err
	}
	if err := bindExchanges(ch, ExchangePublish, "svc_"+svcName, "#"); err != nil {
		return err
	}

	// backoff is an exchange that holds all the retry queues
	if err := createExchange(ch, ExchangeBackoff); err != nil {
		return err
	}
	for _, dur := range BackoffDurations {
		bq, err := createQueue(ch, fmt.Sprint("retry.", dur), amqp.Table{
			"x-dead-letter-exchange": ExchangeRetry,
			"x-message-ttl":          dur.Milliseconds(),
		})
		if err != nil {
			return err
		}
		if err := bindQueue(ch, bq, ExchangeBackoff, buildRetryTopic(dur, "#")); err != nil {
			return err
		}
	}

	// retry takes in all events coming from the backoff exchange and forwards them to the appropriate
	// service/node exchange
	if err := createExchange(ch, ExchangeRetry); err != nil {
		return err
	}
	if err := bindExchanges(ch, ExchangeRetry, "svc_"+svcName, buildRetryTopic("#", svcName)); err != nil {
		return err
	}

	return nil
}

func buildRetryTopic(dur, svcName any) string {
	return fmt.Sprint("RETRY.", dur, ".svc.", svcName)
}

func consumeEvents(ctx context.Context, subCh, pubCh *amqp.Channel, svcName string) error {
	msgs, err := createConsumer(ctx, subCh, svcName, svcName)
	if err != nil {
		return err
	}

	go func() {
		for msg := range msgs {
			handleEvent(ctx, pubCh, svcName, msg)
			msg.Ack(false)
		}
	}()

	return nil
}

func handleEvent(ctx context.Context, ch *amqp.Channel, svcName string, msg amqp.Delivery) {
	original := msg.RoutingKey
	if v, found := msg.Headers["x-original-routing-key"]; found {
		original = v.(string)
	}

	log.Printf("topic: %s\noriginal Topic: %s\nmessage: %s", msg.RoutingKey, original, string(msg.Body))

	var retryCount int32
	if v, found := msg.Headers["x-retry-count"]; found {
		retryCount = v.(int32)
	}

	retryMsg := amqp.Publishing{
		ContentType: msg.ContentType,
		Body:        msg.Body,
		Headers:     msg.Headers,
	}
	log.Print(retryMsg.Headers)

	if int(retryCount) >= len(BackoffDurations) {
		if err := publishEvent(ctx, ch, ExchangeDlx, "error", retryMsg); err != nil {
			log.Printf("publish dlx: %s", err)
		}
		return
	}

	retryCount++
	if retryMsg.Headers == nil {
		retryMsg.Headers = amqp.Table{}
	}
	retryMsg.Headers["x-original-routing-key"] = original
	retryMsg.Headers["x-retry-count"] = retryCount

	err := publishEvent(
		ctx,
		ch,
		ExchangeBackoff,
		buildRetryTopic(BackoffDurations[retryCount-1], svcName),
		retryMsg,
	)
	if err != nil {
		log.Printf("publish retry: %s", err)
	}
}
