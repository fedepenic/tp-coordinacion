package inner

import (
	"encoding/json"
	"errors"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type envelope struct {
	Id   string        `json:"id"`
	Data []interface{} `json:"data"`
}

func SerializeMessage(clientId string, fruitRecords []fruititem.FruitItem) (*middleware.Message, error) {
	data := []interface{}{}
	for _, fruitRecord := range fruitRecords {
		datum := []interface{}{
			fruitRecord.Fruit,
			fruitRecord.Amount,
		}
		data = append(data, datum)
	}

	env := envelope{Id: clientId, Data: data}
	body, err := json.Marshal(env)
	if err != nil {
		return nil, err
	}
	message := middleware.Message{Body: string(body)}
	return &message, nil
}

func DeserializeMessage(message *middleware.Message) (string, []fruititem.FruitItem, bool, error) {
	var env envelope
	if err := json.Unmarshal([]byte(message.Body), &env); err != nil {
		return "", nil, false, err
	}

	fruitRecords := []fruititem.FruitItem{}
	for _, datum := range env.Data {
		fruitPair, ok := datum.([]interface{})
		if !ok {
			return "", nil, false, errors.New("Datum is not an array")
		}

		fruit, ok := fruitPair[0].(string)
		if !ok {
			return "", nil, false, errors.New("Datum is not a (fruit, amount) pair")
		}

		fruitAmount, ok := fruitPair[1].(float64)
		if !ok {
			return "", nil, false, errors.New("Datum is not a (fruit, amount) pair")
		}

		fruitRecord := fruititem.FruitItem{Fruit: fruit, Amount: uint32(fruitAmount)}
		fruitRecords = append(fruitRecords, fruitRecord)
	}

	return env.Id, fruitRecords, len(fruitRecords) == 0, nil
}
