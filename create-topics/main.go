package main

import (
	"flag"
	"github.com/IBM/sarama"
)

const (
	TopicsCreate = "create"
	TopicsDelete = "delete"
)

func main() {

	admin, _ := sarama.NewClusterAdmin([]string{"localhost:9092"}, nil)
	defer admin.Close()

	switch *flag.String("type", TopicsCreate, "type of operation with kafka cluster admin") {
	case TopicsCreate:
		CreateAllTopics(admin)
	case TopicsDelete:
		DeleteAllTopics(admin)
	}
}

func DeleteAllTopics(admin sarama.ClusterAdmin) {
	admin.DeleteTopic("topic1")
	admin.DeleteTopic("topic2")
}

func CreateAllTopics(admin sarama.ClusterAdmin) {
	admin.CreateTopic("topic1", &sarama.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}, false)

	admin.CreateTopic("topic2", &sarama.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}, false)
}
