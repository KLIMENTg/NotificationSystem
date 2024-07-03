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
	"math/rand/v2"
	"net/smtp"
	"os"

	"example.com/projectsolution/project/kafkawrapper"
	"example.com/projectsolution/project/models"
)

const (
	maxEmailRetries      int = 5
	debugErrorPercentage int = 90
)

// Hook called to spawn an email thread
func EmailNotificationRequest(notification *models.Notification) {
	go emailService(notification)
}

// Send an email and attempt retries according to user spec/max retries set in the server
func emailService(notification *models.Notification) {

	// Send emails until the maxEmailRetries or notification.NumOfRepetitions, whichever occurs first
	for emailSendCount := 0; emailSendCount <= maxEmailRetries; emailSendCount++ {

		// Send email and update the 'notification' object
		notification = sendEmail(notification)

		if notification.IsSent {
			// Send success
			err := kafkawrapper.SendKafkaMessage(kafkaTopicProcessed, *notification)
			if err != nil {
				return
			}
			return
		}

		// If we are above the number of retries set by the user
		if !notification.IsSent && notification.NumOfRepetitions >= notification.MaxRetryAttempts {
			notification.IsSent = false

			notification.FailReason =
				"Too many failed attempts. Last attempt failed with: " + notification.FailReason
			kafkawrapper.SendKafkaMessage(kafkaTopicProcessed, *notification)
			return
		}

		// If we are at the max number of retries constant set by our program (last loop iteration)
		if !notification.IsSent && notification.NumOfRepetitions == maxEmailRetries {
			notification.IsSent = false

			notification.FailReason =
				"Too many failed attempts. Max number of retries reached. Last attempt failed with: " + notification.FailReason
			kafkawrapper.SendKafkaMessage(kafkaTopicProcessed, *notification)
			return
		}
	}
}

// Send the email message and return the updated 'notification' object with pass/fail
func sendEmail(notification *models.Notification) *models.Notification {

	// Choose auth method and set it up
	var tempGmailToken string = os.Getenv("NS_EMAIL_TOKEN")
	emailUsername := "infos6587"
	emailDomain := "gmail.com"
	fullEmail := emailUsername + "@" + emailDomain
	gmailUsername := "Info"
	gmailSmtp := "smtp.gmail.com"
	auth := smtp.PlainAuth(gmailUsername, emailUsername, tempGmailToken, gmailSmtp)

	// Here we do it all: connect to our server, set up a message and send it
	emailRecipient := notification.Recipient
	to := []string{emailRecipient}

	// Form email message
	emailSubject := "Subject: Email Notification System\r\n"
	emailBody := notification.Message
	// Hardcoded for non-spam / Otherwise we get 'undisclosed recipients'
	emailRecipientDisclosed := "To: " + emailRecipient + "\r\n"
	msg := []byte(emailRecipientDisclosed + emailSubject + "\r\n" + emailBody)

	// Fire email
	smtpPort := "587"
	err := smtp.SendMail(gmailSmtp+":"+smtpPort, auth, fullEmail, to, msg)
	if err != nil {
		notification.IsSent = false
		notification.NumOfRepetitions = notification.NumOfRepetitions + 1
		notification.FailReason = fmt.Sprintf("failed to send email with following error %s", err)
		return notification
	}

	// Success
	notification.IsSent = true
	return notification

	// // ==== Test code ====
	// notification.IsSent = true

	// randNum := randRange(1, 100)
	// if randNum < debugErrorPercentage {
	// 	notification.IsSent = false
	// 	notification.FailReason = "Failed for some reason"
	// }

	// notification.NumOfRepetitions++

	// return notification
}

// DEBUG: function to generate random numbers
func randRange(min, max int) int {
	return rand.IntN(max-min) + min
}
