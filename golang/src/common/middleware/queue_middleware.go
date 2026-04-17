package middleware

import (
	"context"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

var _ Middleware = (*QueueMiddleware)(nil)

type QueueMiddleware struct {
	conn      *amqp.Connection
	ch        *amqp.Channel
	queueName string
	stopCh    chan struct{}
}

func NewQueueMiddleware(queueName string, connectionSettings ConnSettings) (Middleware, error) {
	connStr := fmt.Sprintf("amqp://guest:guest@%s:%d/", connectionSettings.Hostname, connectionSettings.Port)
	conn, err := amqp.Dial(connStr)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrMessageMiddlewareDisconnected, err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("%w: %v", ErrMessageMiddlewareDisconnected, err)
	}

	_, err = ch.QueueDeclare(
		queueName,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("%w: %v", ErrMessageMiddlewareMessage, err)
	}

	return &QueueMiddleware{
		conn:      conn,
		ch:        ch,
		queueName: queueName,
		stopCh:    make(chan struct{}),
	}, nil
}

func (qm *QueueMiddleware) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
	deliveries, err := qm.ch.Consume(
		qm.queueName,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrMessageMiddlewareDisconnected, err)
	}

	for {
		select {
		case <-qm.stopCh:
			return nil
		case delivery, ok := <-deliveries:
			if !ok {
				return ErrMessageMiddlewareDisconnected
			}
			msg := Message{Body: string(delivery.Body)}
			ack := func() { delivery.Ack(false) }
			nack := func() { delivery.Nack(false, true) }
			callbackFunc(msg, ack, nack)
		}
	}
}

func (qm *QueueMiddleware) StopConsuming() error {
	select {
	case <-qm.stopCh:
	default:
		close(qm.stopCh)
	}
	return nil
}

func (qm *QueueMiddleware) SendWithKey(msg Message, key string) error {
	return qm.Send(msg)
}

func (qm *QueueMiddleware) Send(msg Message) error {
	if err := qm.ch.PublishWithContext(
		context.Background(),
		"",
		qm.queueName,
		false,
		false,
		amqp.Publishing{
			ContentType:  "text/plain",
			DeliveryMode: amqp.Persistent,
			Body:         []byte(msg.Body),
		},
	); err != nil {
		return fmt.Errorf("%w: %v", ErrMessageMiddlewareMessage, err)
	}
	return nil
}

func (qm *QueueMiddleware) Close() error {
	if err := qm.ch.Close(); err != nil {
		return ErrMessageMiddlewareClose
	}
	if err := qm.conn.Close(); err != nil {
		return ErrMessageMiddlewareClose
	}
	return nil
}
