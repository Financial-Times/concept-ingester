package main

import (
	queueConsumer "github.com/Financial-Times/message-queue-gonsumer/consumer"
	"testing"
	//"github.com/stretchr/testify/assert"
)

var writerSlice = []string{"localhost:8080/__people-rw-neo4j-blue", "localhost:8080/__organisations-rw-neo4j-blue"}
var fullMessage = queueConsumer.Message{
	Headers: map[string]string{
		"Content-Type":      "application/json",
		"Message-Id":        "5e0ad5e5-c3d4-387d-9875-ec15501808e5",
		"Message-Timestamp": "2016-06-16T08:14:36.910Z",
		"Message-Type":      "organisations",
		"Origin-System-Id":  "http://cmdb.ft.com/systems/upp",
		"X-Request-Id":      "tid_DwhdCkumqk",
	},
	Body: `{"uuid":"5e0ad5e5-c3d4-387d-9875-ec15501808e5","properName":"Acteon Group Ltd","type":"Organisation","identifiers":[{"authority":"http://api.ft.com/system/FT-TME","identifierValue":"N2I3NzcyNzktNWY4Yy00MGY3LThkMzgtY2M1MDM4MTExOTJh-T04="},{"authority":"http://api.ft.com/system/FT-UPP","identifierValue":"5e0ad5e5-c3d4-387d-9875-ec15501808e5"}]}`}

func TestBuildCorrectRequestURL(t *testing.T) {
	httpConfigurations := httpConfigurations{baseURLSlice: writerSlice}
	httpConfigurations.readMessage(fullMessage)
	//assert := assert.New(t)
}
