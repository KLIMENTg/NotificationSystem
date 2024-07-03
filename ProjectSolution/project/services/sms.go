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

package services

import (
	"fmt"
	"net/http"
	"os"

	"github.com/nexmo-community/nexmo-go"

	"example.com/projectsolution/project/kafkawrapper"
	"example.com/projectsolution/project/models"
)

// Hook called to spawn a SMS thread
func SmsNotificationRequest(notification *models.Notification) {
	go smsService(notification)
}

func smsService(notification *models.Notification) {

	// Send email and update the 'notification' object
	notification = sendSms(notification)

	if notification.IsSent {
		// Send success
		err := kafkawrapper.SendKafkaMessage(kafkaTopicProcessed, *notification)
		if err != nil {
			return
		}
		return
	}

	// Send unsuccessful
	if !notification.IsSent {
		kafkawrapper.SendKafkaMessage(kafkaTopicProcessed, *notification)
		return
	}
}

// Send the sms message and return the updated 'notification' object with pass/fail
func sendSms(notification *models.Notification) *models.Notification {

	var apiKey string = os.Getenv("NS_SMS_API_KEY")
	var apiSecret string = os.Getenv("NS_SMS_API_SECRET")

	// Auth
	auth := nexmo.NewAuthSet()
	auth.SetAPISecret(apiKey, apiSecret)

	// Init Nexmo
	client := nexmo.NewClient(http.DefaultClient, auth)

	// SMS
	SenderTelephone := os.Getenv("NS_SMS_SENDER_TELEPHONE")
	RecipientTelephone := os.Getenv("NS_SMS_RECEIVER_TELEPHONE")
	smsContent := nexmo.SendSMSRequest{
		From: SenderTelephone,
		To:   RecipientTelephone,
		Text: notification.Message}

	smsResponse, _, err := client.SMS.SendSMS(smsContent)
	if err != nil {
		notification.IsSent = false
		notification.NumOfRepetitions = notification.NumOfRepetitions + 1
		notification.FailReason = fmt.Sprintf("failed to send sms with following error %s and status %s.",
			err, smsResponse.Messages[0].Status)
		return notification
	}

	// Success
	notification.IsSent = true
	return notification
}
