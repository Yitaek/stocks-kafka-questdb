package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type StockData struct {
	Price float64 `json:"c"`
}

type StockDataWithTime struct {
	Symbol    string  `json:"symbol"`
	Price     float64 `json:"price"`
	Timestamp int64   `json:"timestamp"`
}

func main() {
	// Create a new Kafka producer instance
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost:9092"})
	if err != nil {
		panic(fmt.Sprintf("Failed to create producer: %s\n", err))
	}
	defer p.Close()

	for {
		token, found := os.LookupEnv("FINNHUB_TOKEN")
		if !found {
			panic("FINNHUB_TOKEN is undefined")
		}
		symbol := "TSLA"

		url := fmt.Sprintf("https://finnhub.io/api/v1/quote?symbol=%s&token=%s", symbol, token)

		// Retrieve the stock data
		resp, err := http.Get(url)
		if err != nil {
			fmt.Println(err)
			return
		}
		defer resp.Body.Close()

		// Read the response body
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			fmt.Println(err)
			return
		}

		// Unmarshal the JSON data into a struct
		var data StockData
		err = json.Unmarshal(body, &data)
		if err != nil {
			fmt.Println(err)
			return
		}

		// Format data with timestamp
		tsData := StockDataWithTime{
			Symbol:    symbol,
			Price:     data.Price,
			Timestamp: time.Now().UnixNano() / 1000000,
		}

		jsonData, err := json.Marshal(tsData)
		if err != nil {
			fmt.Println(err)
			return
		}

		topic := fmt.Sprintf("topic_%s", symbol)
		err = p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          jsonData,
		}, nil)

		if err != nil {
			fmt.Printf("Failed to produce message: %s\n", err)
		}

		fmt.Printf("Message published to Kafka: %s", string(jsonData))

		time.Sleep(30 * time.Second)
	}
}
