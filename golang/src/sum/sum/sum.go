package sum

import (
	"fmt"
	"hash/fnv"
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
	inputQueue      middleware.Middleware
	outputExchanges map[string]middleware.Middleware
	eofBroadcast    middleware.Middleware
	eofReceiver     middleware.Middleware
	aggregationKeys []string
	clientMaps      map[string]map[string]fruititem.FruitItem
	mu              sync.Mutex
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

	outputExchanges := make(map[string]middleware.Middleware, config.AggregationAmount)
	for _, key := range outputExchangeRouteKeys {
		ex, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, []string{key}, connSettings)
		if err != nil {
			inputQueue.Close()
			for _, e := range outputExchanges {
				e.Close()
			}
			return nil, err
		}
		outputExchanges[key] = ex
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
		for _, e := range outputExchanges {
			e.Close()
		}
		return nil, err
	}

	// EOF receiver: subscribes only to this Sum's routing key
	ownKey := []string{fmt.Sprintf("%s_%d", config.SumPrefix, config.Id)}
	eofReceiver, err := middleware.CreateExchangeMiddleware(eofExchangeName, ownKey, connSettings)
	if err != nil {
		inputQueue.Close()
		for _, e := range outputExchanges {
			e.Close()
		}
		eofBroadcast.Close()
		return nil, err
	}

	return &Sum{
		inputQueue:      inputQueue,
		outputExchanges: outputExchanges,
		eofBroadcast:    eofBroadcast,
		eofReceiver:     eofReceiver,
		aggregationKeys: outputExchangeRouteKeys,
		clientMaps:      map[string]map[string]fruititem.FruitItem{},
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

	// Send each fruit to its specific Aggregator exchange instance
	for key := range clientMap {
		fruitRecord := []fruititem.FruitItem{clientMap[key]}
		message, err := inner.SerializeMessage(clientId, fruitRecord)
		if err != nil {
			return err
		}
		routingKey := sum.routingKeyForFruit(key)
		if err := sum.outputExchanges[routingKey].Send(*message); err != nil {
			return err
		}
	}

	// Broadcast EOF to ALL Aggregators
	eofMessage, err := inner.SerializeMessage(clientId, []fruititem.FruitItem{})
	if err != nil {
		return err
	}
	for _, ex := range sum.outputExchanges {
		if err := ex.Send(*eofMessage); err != nil {
			return err
		}
	}

	delete(sum.clientMaps, clientId)
	return nil
}

func (sum *Sum) routingKeyForFruit(fruit string) string {
	h := fnv.New32a()
	h.Write([]byte(fruit))
	return sum.aggregationKeys[h.Sum32()%uint32(len(sum.aggregationKeys))]
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
