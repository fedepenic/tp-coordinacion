package sum

import (
	"fmt"
	"log/slog"
	"sync"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type SumConfig struct {
	Id                int
	MomHost           string
	MomPort           int
	InputQueue        string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
}

type Sum struct {
	inputQueue     middleware.Middleware
	outputExchange middleware.Middleware
	eofBroadcast   middleware.Middleware
	eofReceiver    middleware.Middleware
	clientMaps     map[string]map[string]fruititem.FruitItem
	mu             sync.Mutex
}

func NewSum(config SumConfig) (*Sum, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	outputExchangeRouteKeys := make([]string, config.AggregationAmount)
	for i := range config.AggregationAmount {
		outputExchangeRouteKeys[i] = fmt.Sprintf("%s_%d", config.AggregationPrefix, i)
	}

	outputExchange, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, outputExchangeRouteKeys, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	// EOF broadcast exchange: publishes to all Sum routing keys
	eofExchangeName := config.SumPrefix + "_eof"
	allSumKeys := make([]string, config.SumAmount)
	for i := range config.SumAmount {
		allSumKeys[i] = fmt.Sprintf("%s_%d", config.SumPrefix, i)
	}
	eofBroadcast, err := middleware.CreateExchangeMiddleware(eofExchangeName, allSumKeys, connSettings)
	if err != nil {
		inputQueue.Close()
		outputExchange.Close()
		return nil, err
	}

	// EOF receiver: subscribes only to this Sum's routing key
	ownKey := []string{fmt.Sprintf("%s_%d", config.SumPrefix, config.Id)}
	eofReceiver, err := middleware.CreateExchangeMiddleware(eofExchangeName, ownKey, connSettings)
	if err != nil {
		inputQueue.Close()
		outputExchange.Close()
		eofBroadcast.Close()
		return nil, err
	}

	return &Sum{
		inputQueue:     inputQueue,
		outputExchange: outputExchange,
		eofBroadcast:   eofBroadcast,
		eofReceiver:    eofReceiver,
		clientMaps:     map[string]map[string]fruititem.FruitItem{},
	}, nil
}

func (sum *Sum) Run() {
	go sum.eofReceiver.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		sum.handleEof(msg, ack)
	})

	sum.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		sum.handleMessage(msg, ack, nack)
	})
}

func (sum *Sum) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	clientId, fruitRecords, isEof, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if isEof {
		// Don't process the EOF directly — broadcast it so all Sums receive it
		if err := sum.eofBroadcast.Send(msg); err != nil {
			slog.Error("While broadcasting EOF", "err", err, "clientId", clientId)
		}
		return
	}

	sum.mu.Lock()
	defer sum.mu.Unlock()

	if err := sum.handleDataMessage(clientId, fruitRecords); err != nil {
		slog.Error("While handling data message", "err", err)
	}
}

func (sum *Sum) handleEof(msg middleware.Message, ack func()) {
	defer ack()

	clientId, _, _, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing EOF from exchange", "err", err)
		return
	}

	sum.mu.Lock()
	defer sum.mu.Unlock()

	if err := sum.flushClient(clientId); err != nil {
		slog.Error("While flushing client data", "err", err, "clientId", clientId)
	}
}

func (sum *Sum) flushClient(clientId string) error {
	slog.Info("Flushing client data", "clientId", clientId)

	clientMap, ok := sum.clientMaps[clientId]
	if !ok {
		clientMap = map[string]fruititem.FruitItem{}
	}

	for key := range clientMap {
		fruitRecord := []fruititem.FruitItem{clientMap[key]}
		message, err := inner.SerializeMessage(clientId, fruitRecord)
		if err != nil {
			return err
		}
		if err := sum.outputExchange.Send(*message); err != nil {
			return err
		}
	}

	eofMessage, err := inner.SerializeMessage(clientId, []fruititem.FruitItem{})
	if err != nil {
		return err
	}
	if err := sum.outputExchange.Send(*eofMessage); err != nil {
		return err
	}

	delete(sum.clientMaps, clientId)
	return nil
}

func (sum *Sum) handleDataMessage(clientId string, fruitRecords []fruititem.FruitItem) error {
	if _, ok := sum.clientMaps[clientId]; !ok {
		sum.clientMaps[clientId] = map[string]fruititem.FruitItem{}
	}

	for _, fruitRecord := range fruitRecords {
		if existing, ok := sum.clientMaps[clientId][fruitRecord.Fruit]; ok {
			sum.clientMaps[clientId][fruitRecord.Fruit] = existing.Sum(fruitRecord)
		} else {
			sum.clientMaps[clientId][fruitRecord.Fruit] = fruitRecord
		}
	}
	return nil
}
