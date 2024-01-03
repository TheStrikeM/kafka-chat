package main

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/IBM/sarama"
	"log"
	"log/slog"
	"os"
	"sync"
)

const (
	ProduceTopic = "topic2"
	ConsumeTopic = "topic1"

	TranslationConsume = "consume"
	TranslationProduce = "produce"
)

func main() {
	//user2 produce to topic2
	//user2 consume in topic1
	typePtr := flag.String("type", TranslationConsume, "secret")
	flag.Parse()

	switch *typePtr {
	case TranslationConsume:
		RunConsumeModule()
	case TranslationProduce:
		RunProduceModule()
	}
	fmt.Println(*typePtr)
}

func RunConsumeModule() {
	slog.Info("SYSTEM | Hello, user2! This is watching module")

	consumer, _ := sarama.NewConsumer([]string{"localhost:9092"}, nil)
	defer consumer.Close()

	var wg sync.WaitGroup

	partitionConsumeList, _ := consumer.Partitions(ConsumeTopic)
	for _, partition := range partitionConsumeList {
		pc, _ := consumer.ConsumePartition(ConsumeTopic, partition, sarama.OffsetOldest)
		wg.Add(1)

		go func(pc sarama.PartitionConsumer) {
			defer wg.Done()
			for message := range pc.Messages() {
				log.Print(string(message.Value))
			}
		}(pc)
	}

	partitionProduceList, _ := consumer.Partitions(ProduceTopic)
	for _, partition := range partitionProduceList {
		pc, _ := consumer.ConsumePartition(ProduceTopic, partition, sarama.OffsetOldest)
		wg.Add(1)

		go func(pc sarama.PartitionConsumer) {
			defer wg.Done()
			for message := range pc.Messages() {
				log.Print(string(message.Value))
			}
		}(pc)
	}

	wg.Wait()
}

func RunProduceModule() {
	slog.Info("SYSTEM | Hello, user2! This is typing module. Enter the message to user1")

	reader := bufio.NewReader(os.Stdin)
	producer, _ := sarama.NewSyncProducer([]string{"localhost:9092"}, nil)
	for {
		body, _ := reader.ReadString('\n')
		currentMessage := &sarama.ProducerMessage{
			Topic: ProduceTopic,
			Value: sarama.StringEncoder("[USER2] -> [USER1]: " + body),
		}

		partition, offset, _ := producer.SendMessage(currentMessage)
		log.Printf("Sent to partion %v and the offset is %v", partition, offset)
	}
}
