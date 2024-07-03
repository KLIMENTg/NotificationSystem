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
	"os"

	"github.com/slack-go/slack"

	"example.com/projectsolution/project/kafkawrapper"
	"example.com/projectsolution/project/models"
)

// Hook called to spawn a slack thread
func SlackNotificationRequest(notification *models.Notification) {
	go slackService(notification)
}

func slackService(notification *models.Notification) {

	// Send email and update the 'notification' object
	notification = sendSlack(notification)

	if notification.IsSent {
		// Send success
		err := kafkawrapper.SendKafkaMessage(kafkaTopicProcessed, *notification)
		if err != nil {
			return
		}
		return
	}

	// If we are above the number of retries set by the user
	if !notification.IsSent {
		notification.FailReason = "Failed to send Slack with"
		kafkawrapper.SendKafkaMessage(kafkaTopicProcessed, *notification)
		return
	}
}

// Send the slack message and return the updated 'notification' object with pass/fail
func sendSlack(notification *models.Notification) *models.Notification {

	var slackChannel string = os.Getenv("NS_SLACK_CHANNEL")
	var slackBotToken string = os.Getenv("NS_SLACK_BOT_TOKEN")

	slackApi := slack.New(slackBotToken)

	_, _, err := slackApi.PostMessage(
		slackChannel,
		slack.MsgOptionText(notification.Message, false),
	)
	if err != nil {
		notification.IsSent = false
		notification.NumOfRepetitions = notification.NumOfRepetitions + 1
		notification.FailReason = fmt.Sprintf("failed to send slack message with following error %s.", err)
		return notification
	}

	// Success
	notification.IsSent = true
	return notification
}
