package main

import (
	"errors"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type Kafka struct {
	servers  string
	group    string
	topic    string
	topicReg string
}

func (k *Kafka) Consume() error {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": k.servers,
		"group.id":          k.group,
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		return err
	}

	defer c.Close()

	err = c.SubscribeTopics([]string{k.topic}, nil)
	if err != nil {
		return err
	}

	// A signal handler or similar could be used to set this to false to break the loop.
	run := true
	for run {
		msg, err := c.ReadMessage(time.Second * 5)
		if err != nil {
			fmt.Println("Error is:", err)
			return err
		}
		if msg != nil {
			return nil
		}
	}

	return nil
}

func (k *Kafka) Produce(msgs []string) error {
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": k.servers})
	if err != nil {
		return err
	}
	defer p.Close()

	ch := make(chan error)
	// Listen to all the events on the default events channel
	go func(ch chan error) {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				m := ev
				if m.TopicPartition.Error != nil {
					ch <- m.TopicPartition.Error
					fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
				} else {
					fmt.Printf("Delivered message to topic %s [%d] at offset %v\n",
						*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
					ch <- nil
				}
			case kafka.Error:
				ch <- errors.New(ev.String())
			default:
				fmt.Printf("Ignored event: %s\n", ev)
			}
		}
		close(ch)
	}(ch)

	// Produce messages to topic (asynchronously)
	topic := k.topic
	for _, word := range msgs {
		err := p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          []byte(word),
		}, nil)
		if err != nil {
			return err
		}
	}

	err = <-ch
	if err != nil {
		return err
	}

	// Wait for message deliveries before shutting down
	p.Flush(2 * 1000)
	return nil
}
