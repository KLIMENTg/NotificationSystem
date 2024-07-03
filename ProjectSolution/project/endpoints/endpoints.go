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

package endpoints

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"example.com/projectsolution/project/kafkawrapper"
	"example.com/projectsolution/project/models"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

const (
	ProducerPort            = ":8080"
	kafkaTopicProcessed     = "processed"
	maxNumberDefaultRetries = "5"
	hardTimeout             = 60
)

// ====== NOTIFICATION STORAGE ======
type MessageNotification map[uuid.UUID]models.Notification

type NotificationStore struct {
	data MessageNotification
	mu   sync.RWMutex
}

// Create the 'database' for messages
var notificationStore = NotificationStore{
	data: make(MessageNotification),
}

// Loads messages onto the store, while tagging each message with a messageID
func (ns *NotificationStore) Add(notification models.Notification) (messageID uuid.UUID, err error) {
	ns.mu.Lock()
	defer ns.mu.Unlock()

	maxChecks := 500
	// Check for duplicates
	for attempt := 0; attempt <= maxChecks; attempt++ {
		messageID = uuid.New()
		if _, exists := ns.data[messageID]; !exists {
			// Assign timestamp and messageID
			notification.TimeStamp = time.Now()
			notification.MessageID = messageID
			ns.data[messageID] = notification
			return messageID, nil
		}
	}
	return uuid.UUID{}, fmt.Errorf("Could not find a free key to insert into map")
}

// Update the store with an updated notification
func (ns *NotificationStore) Update(messageID uuid.UUID, notification models.Notification) {
	ns.mu.Lock()
	defer ns.mu.Unlock()

	ns.data[messageID] = notification
}

// Delete the item from the store
func (ns *NotificationStore) Delete(messageID uuid.UUID) {
	ns.mu.Lock()
	defer ns.mu.Unlock()

	if _, exists := ns.data[messageID]; exists {
		delete(ns.data, messageID)
	}
}

// Retrieves messages from the store, using the messageID to identify the correct message
func (ns *NotificationStore) Get(messageID uuid.UUID) models.Notification {
	ns.mu.RLock()
	defer ns.mu.RUnlock()
	return ns.data[messageID]
}

func SetupEndpoints() {
	gin.SetMode(gin.ReleaseMode)
	router := gin.Default()
	router.POST("/notification", notificationHandler())

	if err := router.Run(ProducerPort); err != nil {
		log.Printf("failed to run the server: %v", err)
	}
}

// Function gets the results from the kafka topics
func GetResults(ctx context.Context, messageID uuid.UUID, wasSuccessful chan bool) {

	// Check every 100ms or until we timeout at the caller
	ticker := time.NewTicker(time.Millisecond * 100)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Check the messageID we expect to receive
			resultMsg := notificationStore.Get(messageID)

			// Check if our message has been processed
			if resultMsg.IsSent {
				wasSuccessful <- true
			}

			// Check if we have a retry failure
			if resultMsg.FailReason != "" {
				wasSuccessful <- false
			}

		}
	}
}

// Updates the Notification Store with all processed notifications
func ReceiveProcessedNotification(receivedNotification *models.Notification) {
	notificationStore.Update(receivedNotification.MessageID, *receivedNotification)
}

// End-point handler for all 'notification' requests
// Dispatches Kafka messages on the appropriate topics
func notificationHandler() gin.HandlerFunc {

	// Continuously get results from the 'processed' topic
	ctx := context.Background()
	go kafkawrapper.ReceiveKafkaMessage(ctx, kafkaTopicProcessed, ReceiveProcessedNotification)

	return func(ctx *gin.Context) {

		// Checking the validity of the request

		// Check if required parameter 'mode' is sent
		mode := ctx.PostForm("mode")
		if mode == "" || (mode != "email" && mode != "sms" && mode != "slack") {
			ctx.JSON(http.StatusBadRequest, gin.H{"message": "Mode is either blank or not one of the supported modes: 'email', 'sms' or 'slack'"})
			return
		}

		// Check if required parameter 'message' is sent
		message := ctx.PostForm("message")
		if message == "" {
			ctx.JSON(http.StatusBadRequest, gin.H{"message": "Message is blank"})
			return
		}

		// Check if optional parameter 'max_retry_attempts' is sent
		max_retry_attempts := ctx.PostForm("max_retry_attempts")
		if max_retry_attempts == "" {
			max_retry_attempts = maxNumberDefaultRetries
		}
		maxRetryAttempts, err := strconv.Atoi(max_retry_attempts)
		if err != nil {
			ctx.JSON(http.StatusBadRequest, gin.H{"message": "'max_retry_attempts' is not an integer"})
			return
		}

		// Check if optional parameter 'recipient' is sent
		// For now recipient only works for email. Can do a basic regex check for email syntax.
		recipient := ctx.PostForm("recipient")
		if mode == "email" && recipient == "" {
			recipient = os.Getenv("NS_EMAIL_DEFAULT_RECIPIENT")
		}

		// Add it to the store for reference
		messageID, err := notificationStore.Add(models.Notification{mode, message, maxRetryAttempts,
			recipient, time.Time{}, uuid.UUID{}, 0, false, ""})
		if err != nil {
			ctx.JSON(http.StatusInternalServerError, gin.H{"message": "Internal server error"})
			return
		}

		// Send for Processing
		kafkaTopic := mode
		err = kafkawrapper.SendKafkaMessage(kafkaTopic, notificationStore.Get(messageID))
		if err != nil {
			ctx.JSON(http.StatusInternalServerError, gin.H{"message": "Internal server error"})
			return
		}

		// Receive the Processing
		resultCtx, cancel := context.WithCancel(context.Background())
		defer cancel()

		wasSuccessfulChan := make(chan bool)
		go GetResults(resultCtx, messageID, wasSuccessfulChan)

		// Wait for a success or failure from our services. Or a hard timeout
		select {
		case isSuccess := <-wasSuccessfulChan:
			if isSuccess {
				// Send success
				ctx.JSON(http.StatusOK, gin.H{
					"message": "Notification sent successfully!",
				})
			} else {
				// Send failure
				ctx.JSON(http.StatusRequestTimeout, gin.H{
					"message": fmt.Sprintf("Notification sending failed after max number of attempts. Notification service error: %s",
						notificationStore.Get(messageID).FailReason),
				})
			}
		case <-time.After(hardTimeout * time.Second):
			// Send max timeout error
			ctx.JSON(http.StatusRequestTimeout, gin.H{
				"message": "Notification sending timed out (" + strconv.FormatUint(hardTimeout, 10) + " seconds)",
			})
		}

		notificationStore.Delete(messageID)
	}
}
