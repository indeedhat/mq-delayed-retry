# Delayed retry of rabbitmq failed messages
This is a proof of concept implementation for handling delayed retrys of failed messages in rabbit mq.

It makes use of multiple exchanges/exchange bindings to re publish messages to a specific service should a message fail to properly consume after a set delay without publishing the message back to the whole cluster.

## Initial publish
```mermaid
sequenceDiagram
    actor Publisher
    actor Consumer
    participant PublishExchange
    participant ServiceExchange
    participant DelayQueue
    participant RetryExchange
    Publisher->>PublishExchange: Publish Message
    PublishExchange->>ServiceExchange: Forward to service echanges
    ServiceExchange->>Consumer: picked up via topic
    Consumer->>DelayQueue: send to delay queue with 1s delay topic and 1s ttl
    DelayQueue->>RetryExchange: on message timeout
    RetryExchange->>ServiceExchange: send to appropriate service exchange to retry
    ServiceExchange->>Consumer: picked up via topic
```
