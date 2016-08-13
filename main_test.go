package main

import (
	"testing"

	queueConsumer "github.com/Financial-Times/message-queue-gonsumer/consumer"
	"github.com/stretchr/testify/assert"

	"strings"
)

var peopleService = "people-rw-neo4j-blue"
var organisationsService = "organisations-rw-neo4j-blue"

var correctWriterMappings = map[string]string{
	"people-rw-neo4j-blue":        "http://localhost:8080/__people-rw-neo4j-blue",
	"organisations-rw-neo4j-blue": "http://localhost:8080/__organisations-rw-neo4j-blue",
}

var uuid = "5e0ad5e5-c3d4-387d-9875-ec15501808e5"
var validMessageType = "organisations"
var invalidMessageType = "animals"

//func Test

func TestWriterServiceSliceCreationCluster(t *testing.T) {
	writerMappings := createWriterMappings(peopleService+","+organisationsService, "http://localhost:8080")
	assert := assert.New(t)
	assert.EqualValues(correctWriterMappings, writerMappings, "Should have two mappings")
}

func TestWriterServiceSliceCreationLocal(t *testing.T) {
	writerMappings := createWriterMappings(peopleService+":7070,"+organisationsService+":7080", "http://localhost:8080")
	localWriterMappings := map[string]string{
		"people-rw-neo4j-blue:7070":        "http://localhost:7070",
		"organisations-rw-neo4j-blue:7080": "http://localhost:7080",
	}
	assert := assert.New(t)
	assert.EqualValues(localWriterMappings, writerMappings, "Should have two mappings")
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
	request, reqUrl, err := resolveWriterAndCreateRequest(validMessageType, strings.NewReader(validMessage.Body), uuid, correctWriterMappings)
	assert := assert.New(t)
	assert.NoError(err)
	assert.Equal("http://localhost:8080/__organisations-rw-neo4j-blue/organisations/5e0ad5e5-c3d4-387d-9875-ec15501808e5", reqUrl)
	assert.NotNil(request, "Request is nil")
}

func TestErrorIsThrownWhenIngestionTypeMatchesNoWriters(t *testing.T) {
	validMessage := createMessage(uuid, invalidMessageType)
	_, reqUrl, err := resolveWriterAndCreateRequest(invalidMessageType, strings.NewReader(validMessage.Body), uuid, correctWriterMappings)
	assert := assert.New(t)
	assert.Equal("", reqUrl)
	assert.Error(err, "No configured writer for concept: "+invalidMessageType)
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
