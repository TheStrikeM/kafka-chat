package main

import (
	"bufio"
	"flag"
	"github.com/IBM/sarama"
	"log"
	"log/slog"
	"os"
	"sync"
)

const (
	ProduceTopic = "topic1"
	ConsumeTopic = "topic2"

	TranslationConsume = "consume"
	TranslationProduce = "produce"
)

func main() {
	//user1 produce to topic1
	//user1 consume in topic2

	typePtr := flag.String("type", TranslationConsume, "secret")
	flag.Parse()

	switch *typePtr {
	case TranslationConsume:
		RunConsumeModule()
	case TranslationProduce:
		RunProduceModule()
	}
}

func RunConsumeModule() {
	slog.Info("SYSTEM | Hello, user1! This is watching module")

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
	slog.Info("SYSTEM | Hello, user1! This is typing module. Enter the message to user2")

	reader := bufio.NewReader(os.Stdin)
	producer, _ := sarama.NewSyncProducer([]string{"localhost:9092"}, nil)
	for {
		body, _ := reader.ReadString('\n')
		currentMessage := &sarama.ProducerMessage{
			Topic: ProduceTopic,
			Value: sarama.StringEncoder("[USER1] -> [USER2]: " + body),
		}

		partition, offset, _ := producer.SendMessage(currentMessage)
		log.Printf("Sent to partion %v and the offset is %v", partition, offset)
	}
}
