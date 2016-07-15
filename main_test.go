package main

import (
	queueConsumer "github.com/Financial-Times/message-queue-gonsumer/consumer"
	"testing"
	"github.com/stretchr/testify/assert"

	"strings"
)

var peopleService = "people-rw-neo4j-blue"
var organisationsService = "organisations-rw-neo4j-blue"

var writerSlice []string

var uuid = "5e0ad5e5-c3d4-387d-9875-ec15501808e5"
var validMessageType = "organisations"
var invalidMessageType = "animals"

func TestWriterServiceSliceCreation(t *testing.T) {
	writerSlice = createWritersSlice(peopleService + "," + organisationsService,"http://localhost:8080")
	assert := assert.New(t)
	assert.Contains(writerSlice, "http://localhost:8080/__people-rw-neo4j-blue")
	assert.Contains(writerSlice, "http://localhost:8080/__organisations-rw-neo4j-blue")
}

func TestUuidAndMessageTypeAreExtractedFromMessage(t *testing.T) {
	validMessage := createMessage(uuid, validMessageType)
	ingestionType, uuid := extractMessageTypeAndId(validMessage.Headers)
	assert := assert.New(t)
	assert.Equal("organisations", ingestionType)
	assert.Equal("5e0ad5e5-c3d4-387d-9875-ec15501808e5", uuid)
}

func TestWriterUrlIsResolvedCorrectlyAndRequestIsNotNull(t *testing.T) {
	validMessage := createMessage(uuid, validMessageType)
	request, reqUrl, err := resolveWriterAndCreateRequest(validMessageType, strings.NewReader(validMessage.Body), uuid, writerSlice)
	assert := assert.New(t)
	assert.NoError(err)
	assert.Equal("http://localhost:8080/__organisations-rw-neo4j-blue/organisations/5e0ad5e5-c3d4-387d-9875-ec15501808e5", reqUrl)
	assert.NotNil(request, "Request is nil")
}

func TestErrorIsThrownWhenIngestionTypeMatchesNoWriters(t *testing.T) {
	validMessage := createMessage(uuid, invalidMessageType)
	_, reqUrl, err := resolveWriterAndCreateRequest(invalidMessageType, strings.NewReader(validMessage.Body), uuid, writerSlice)
	assert := assert.New(t)
	assert.Equal("", reqUrl)
	assert.Error(err, "No configured writer for concept: " + invalidMessageType)
}

func createMessage(messageId string, messageType string) queueConsumer.Message {
	return queueConsumer.Message{
		Headers: map[string]string{
			"Content-Type":      "application/json",
			"Message-Id":        messageId,
			"Message-Timestamp": "2016-06-16T08:14:36.910Z",
			"Message-Type":      messageType,
			"Origin-System-Id":  "http://cmdb.ft.com/systems/upp",
			"X-Request-Id":      "tid_newid",
		},
		Body: `{transformed-org-json`}
}

func TestResolveServiceAddress(t *testing.T) {
	assert := assert.New(t)
	vulcan := "http://localhost:8080"

	tests := []struct {
		writer string
		expectedAddress []string
	}{
		{"some_writer:8088", []string {"http://localhost:8088", "some_writer"}},
		{"some_writer", []string {vulcan, "some_writer"}},
	}

	for _, test := range tests {
		actualWriterAddress := resolveServiceAddress(test.writer, vulcan)
		assert.Equal(actualWriterAddress, test.expectedAddress, "writer address expected %v, but was %v ", test.expectedAddress, actualWriterAddress)

	}
}


