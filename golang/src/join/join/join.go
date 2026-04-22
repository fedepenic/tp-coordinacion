package join

import (
	"log/slog"
	"os"
	"os/signal"
	"sort"
	"syscall"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type JoinConfig struct {
	MomHost           string
	MomPort           int
	InputQueue        string
	OutputQueue       string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
	TopSize           int
}

type Join struct {
	inputQueue        middleware.Middleware
	outputQueue       middleware.Middleware
	aggregationAmount int
	topSize           int
	clientTops        map[string][]fruititem.FruitItem
	topCount          map[string]int
}

func NewJoin(config JoinConfig) (*Join, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	return &Join{
		inputQueue:        inputQueue,
		outputQueue:       outputQueue,
		aggregationAmount: config.AggregationAmount,
		topSize:           config.TopSize,
		clientTops:        map[string][]fruititem.FruitItem{},
		topCount:          map[string]int{},
	}, nil
}

func (join *Join) Run() {
	go join.handleSignals()
	join.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		join.handleMessage(msg, ack, nack)
	})
}

func (join *Join) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received")
	join.inputQueue.StopConsuming()
}

func (join *Join) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	clientId, fruitRecords, _, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	join.clientTops[clientId] = append(join.clientTops[clientId], fruitRecords...)
	join.topCount[clientId]++

	slog.Info("Received partial top", "clientId", clientId, "count", join.topCount[clientId], "expected", join.aggregationAmount)

	if join.topCount[clientId] < join.aggregationAmount {
		return
	}

	if err := join.sendFinalTop(clientId); err != nil {
		slog.Error("While sending final top", "err", err, "clientId", clientId)
	}
}

func (join *Join) sendFinalTop(clientId string) error {
	allItems := join.clientTops[clientId]

	sort.SliceStable(allItems, func(i, j int) bool {
		return allItems[j].Less(allItems[i])
	})

	finalTopSize := min(join.topSize, len(allItems))
	finalTop := allItems[:finalTopSize]

	message, err := inner.SerializeMessage(clientId, finalTop)
	if err != nil {
		return err
	}
	if err := join.outputQueue.Send(*message); err != nil {
		return err
	}

	delete(join.clientTops, clientId)
	delete(join.topCount, clientId)
	return nil
}
