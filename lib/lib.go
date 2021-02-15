package lib

import (
	"gopkg.in/tucnak/telebot.v2"
	"os"
)

var Bot *telebot.Bot

func GetEnv(key string, def string) string {
	env := os.Getenv(key)
	if env == "" {
		return def
	}
	return env
}
