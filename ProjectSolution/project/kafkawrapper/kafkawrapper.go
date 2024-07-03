// Copyright (c) 2024 Kliment Gueorguiev

// Permission is hereby granted, free of charge, to any person obtaining a copy of this software and
// associated documentation files (the "Software"), to deal in the Software without restriction,
// including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense,
// and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so,
// subject to the following conditions:

// The above copyright notice and this permission notice shall be included in all copies or substantial
// portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT
// NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
// IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
// WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
// SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

package kafkawrapper

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"example.com/projectsolution/project/models"
	"github.com/IBM/sarama"
)

const (
	producerPort       = ":8080"
	kafkaServerAddress = "localhost:9092"
	consumerGroup      = "notifications-group"
)

// ============== PRODUCER RELATED FUNCTIONS ==============

// Setup the samara producer
func setupProducer() (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer([]string{kafkaServerAddress},
		config)
	if err != nil {
		return nil, fmt.Errorf("failed to setup producer: %w", err)
	}
	return producer, nil
}

// Push a notification to a certain kafka topic
func SendKafkaMessage(topic string, notification models.Notification) error {

	producer, err := setupProducer()
	if err != nil {
		return fmt.Errorf("failed to setup producer: %w", err)
	}

	notificationJSON, err := json.Marshal(notification)
	if err != nil {
		return fmt.Errorf("failed to marshal notification: %w", err)
	}

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(notification.MessageID.String()),
		Value: sarama.StringEncoder(notificationJSON),
	}

	_, _, err = producer.SendMessage(msg)
	if err != nil {
		return fmt.Errorf("failed to sent on kafka topic: %w", err)
	}

	return nil
}

// ============== CONSUMER RELATED FUNCTIONS ==============

// Creates a new samara consumer group
func initializeConsumerGroup() (sarama.ConsumerGroup, error) {
	config := sarama.NewConfig()

	// CAUTION: These constants make it so the `processed` topic doesn't return and we don't get called back in time
	config.Consumer.Offsets.AutoCommit.Enable = true
	config.Consumer.Offsets.AutoCommit.Interval = 1 * time.Second

	consumerGroup, err := sarama.NewConsumerGroup(
		[]string{kafkaServerAddress}, consumerGroup, config)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize consumer group: %w", err)
	}

	return consumerGroup, nil
}

// Samara's ConsumerGroupHandler interface implementation
// Function callback used in the Consumer
type Consumer struct {
	messageCallbackFunction msgCallback
}

func (*Consumer) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (*Consumer) Cleanup(sarama.ConsumerGroupSession) error { return nil }

// Hook/callback for the sarama.ConsumerGroup's Consume() method
// It gets called on every message on the subscribed topic
// Inject/call our own function callback inside the consumer
func (consumer *Consumer) ConsumeClaim(
	sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	for msg := range claim.Messages() {

		var notification models.Notification
		err := json.Unmarshal(msg.Value, &notification)
		if err != nil {
			log.Printf("failed to unmarshal notification: %v", err)
			continue
		}
		// Set the message as consumed
		sess.MarkMessage(msg, "")

		// Callback whatever function was given
		consumer.messageCallbackFunction(&notification)
	}
	return nil
}

// The function signature for the information receiver in ReceiveKafkaMessage()
type msgCallback func(*models.Notification)

// Receive Kafka messages on a certain topic. Upon reception of a message the `messageCallbackFunction`
// gets called with the notification struct filled from the topic
func ReceiveKafkaMessage(ctx context.Context, kafkaTopic string, messageCallbackFunction msgCallback) {

	// Initialize a Consumer Group
	consumerGroup, err := initializeConsumerGroup()
	if err != nil {
		log.Printf("initialization error: %v", err)
	}
	defer consumerGroup.Close()

	consumer := &Consumer{
		messageCallbackFunction: messageCallbackFunction,
	}

	for {
		err = consumerGroup.Consume(ctx, []string{kafkaTopic}, consumer)
		if err != nil {
			log.Printf("error from consumer: %v", err)
		}
		if ctx.Err() != nil {
			return
		}
	}
}
