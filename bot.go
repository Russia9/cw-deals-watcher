package main

import (
	"cw-deals-watcher/lib"
	"cw-deals-watcher/messages"
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/getsentry/sentry-go"
	"github.com/sirupsen/logrus"
	"gopkg.in/tucnak/telebot.v2"
	"strconv"
	"time"
)

func InitBot(telegramToken string, logger *logrus.Logger, consumer *kafka.Consumer) error {
	logger.Debug("Initializing Telegram bot")
	var err error
	lib.Bot, err = telebot.NewBot(
		telebot.Settings{
			Token:  telegramToken,
			Poller: &telebot.LongPoller{Timeout: 10 * time.Second},
		})
	if err != nil {
		return err
	}

	consumer.SubscribeTopics([]string{"cw3-deals"}, nil)
	channel := make(chan messages.DealMessage)
	go Sender(channel)

	for {
		msg, err := consumer.ReadMessage(-1)
		if err == nil {
			var message messages.DealMessage
			err = json.Unmarshal(msg.Value, &message)
			if err != nil {
				sentry.CaptureException(err)
				logger.Error(fmt.Sprintf("Decoder error: %v (%v)\n", err, msg))
			}

			channel <- message

			logger.Trace(fmt.Sprintf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value)))
		} else {
			sentry.CaptureException(err)
			logger.Error(fmt.Sprintf("Consumer error: %v (%v)\n", err, msg))
		}
	}
}

func Sender(channel chan messages.DealMessage) {
	chat, err := lib.Bot.ChatByID(lib.GetEnv("CWDW_CHANNEL_ID", "-1001429147053"))
	if err != nil {
		logger.Error(err)
	}

	messagePool := make(map[string][]messages.DealMessage)
	lastTimestamp := int32(time.Now().Unix())
	counter := 0

	for {
		var message messages.DealMessage
		message = <-channel
		messagePool[message.Item] = append(messagePool[message.Item], message)
		counter++

		if int32(time.Now().Unix())-20 >= lastTimestamp || counter >= 30 {
			msgString := ""
			for item, value := range messagePool {
				msgString += "<b>" + item + "</b>:\n"
				for _, msg := range value {
					msgString += "<code>" + msg.SellerCastle + msg.SellerName + " -> " + msg.BuyerCastle + msg.BuyerName + ", " +
						"" + "" + strconv.Itoa(msg.Quantity) + " x " + strconv.Itoa(msg.Price) + "💰 </code>\n"
				}
			}

			_, err = lib.Bot.Send(chat, msgString, telebot.ModeHTML)
			if err != nil {
				sentry.CaptureException(err)
				logger.Error(err)
			}

			lastTimestamp = int32(time.Now().Unix())
			counter = 0
		}
	}
}
