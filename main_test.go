package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	queueConsumer "github.com/Financial-Times/message-queue-gonsumer/consumer"
	metrics "github.com/rcrowley/go-metrics"
	"github.com/stretchr/testify/assert"
)

var peopleService = "people-rw-neo4j-blue"
var organisationsService = "organisations-rw-neo4j-blue"

var correctWriterMappings = map[string]string{
	"people-rw-neo4j-blue":        "http://localhost:8080/__people-rw-neo4j-blue",
	"organisations-rw-neo4j-blue": "http://localhost:8080/__organisations-rw-neo4j-blue",
}

var uuid = "5e0ad5e5-c3d4-387d-9875-ec15501808e5"
var validMessageTypeOrganisations = "organisations"
var invalidMessageType = "animals"

func TestMessageProcessingHappyPathIncrementsSuccessMeter(t *testing.T) {
	// Test server that always responds with 200 code
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))
	defer server.Close()

	mockedWriterMappings := map[string]string{
		"people-rw-neo4j-blue":        "http://localhost:8080/__people-rw-neo4j-blue",
		"organisations-rw-neo4j-blue": server.URL,
	}

	successMeterInitialCount, failureMeterInitialCount := getCounts()

	ing := ingesterService{baseURLMappings: mockedWriterMappings, client: &http.Client{}}

	err := ing.processMessage(createMessage(uuid, validMessageTypeOrganisations))

	assert := assert.New(t)
	assert.NoError(err, "Should complete without error")

	successMeterFinalCount, failureMeterFinalCount := getCounts()

	assert.True(successMeterFinalCount-successMeterInitialCount == 1, "Should have incremented SuccessCount by 1")
	assert.True(failureMeterFinalCount-failureMeterInitialCount == 0, "Should not have incremented FailureCount")
}

func TestMessageProcessingUnhappyPathIncrementsFailureMeter(t *testing.T) {
	// Test server that always responds with 500 code
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	defer server.Close()

	mockedWriterMappings := map[string]string{
		"people-rw-neo4j-blue":        "http://localhost:8080/__people-rw-neo4j-blue",
		"organisations-rw-neo4j-blue": server.URL,
	}

	successMeterInitialCount, failureMeterInitialCount := getCounts()

	ing := ingesterService{baseURLMappings: mockedWriterMappings, client: &http.Client{}}

	err := ing.processMessage(createMessage(uuid, validMessageTypeOrganisations))

	assert := assert.New(t)
	assert.Error(err, "Should error")

	successMeterFinalCount, failureMeterFinalCount := getCounts()

	assert.True(successMeterFinalCount-successMeterInitialCount == 0, "Should not have incremented SuccessCount")
	assert.True(failureMeterFinalCount-failureMeterInitialCount == 1, "Should have incremented FailureCount by 1")
}

func TestMessageProcessingHappyPathIncrementsSuccessMeterForElasticsearchIncluded(t *testing.T) {
	// Test server that always responds with 200 code
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))
	defer server.Close()

	mockedWriterMappings := map[string]string{
		"people-rw-neo4j-blue":        "http://localhost:8080/__people-rw-neo4j-blue",
		"organisations-rw-neo4j-blue": server.URL,
	}

	successMeterInitialCount, failureMeterInitialCount := getCounts()
	failureMeterInitialCountForElasticsearch := getElasticsearchCount()

	ing := ingesterService{baseURLMappings: mockedWriterMappings, elasticWriterURL: server.URL, client: &http.Client{}}

	err := ing.processMessage(createMessage(uuid, validMessageTypeOrganisations))

	assert := assert.New(t)
	assert.NoError(err, "Should complete without error")

	successMeterFinalCount, failureMeterFinalCount := getCounts()
	failureMeterFinalCountForElasticsearch := getElasticsearchCount()

	assert.True(successMeterFinalCount-successMeterInitialCount == 1, "Should have incremented SuccessCount by 1")
	assert.True(failureMeterFinalCount-failureMeterInitialCount == 0, "Should not have incremented FailureCount")
	assert.True(failureMeterFinalCountForElasticsearch-failureMeterInitialCountForElasticsearch == 0, "Should not have incremented FailureCount")
}

func TestMessageProcessingUnhappyPathIncrementsFailureMeterWithElasticsearch(t *testing.T) {

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/bulk") {
			w.WriteHeader(500)
		} else {
			w.WriteHeader(200)
		}
	}))
	defer server.Close()

	mockedWriterMappings := map[string]string{
		"people-rw-neo4j-blue":        "http://localhost:8080/__people-rw-neo4j-blue",
		"organisations-rw-neo4j-blue": server.URL,
	}

	successMeterInitialCount, failureMeterInitialCount := getCounts()
	failureMeterInitialCountForElasticsearch := getElasticsearchCount()

	ing := ingesterService{baseURLMappings: mockedWriterMappings, elasticWriterURL: server.URL + "/bulk", client: &http.Client{}}

	err := ing.processMessage(createMessage(uuid, validMessageTypeOrganisations))

	assert := assert.New(t)
	assert.Error(err, "Should error")

	successMeterFinalCount, failureMeterFinalCount := getCounts()
	failureMeterFinalCountForElasticsearch := getElasticsearchCount()

	assert.True(successMeterFinalCount-successMeterInitialCount == 0, "Should not have incremented SuccessCount")
	assert.True(failureMeterFinalCount-failureMeterInitialCount == 0, "Should not have incremented FailureCount")
	assert.True(failureMeterFinalCountForElasticsearch-failureMeterInitialCountForElasticsearch == 1, "Should have incremented FailureCount by 1")
}

func getCounts() (int64, int64) {
	successMeter := metrics.GetOrRegisterMeter("organisations-SUCCESS", metrics.DefaultRegistry)
	failureMeter := metrics.GetOrRegisterMeter("organisations-FAILURE", metrics.DefaultRegistry)
	return successMeter.Count(), failureMeter.Count()
}

func getElasticsearchCount() int64 {
	return metrics.GetOrRegisterMeter("organisations-elasticsearch-FAILURE", metrics.DefaultRegistry).Count()
}

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
	validMessage := createMessage(uuid, validMessageTypeOrganisations)
	extractedIngestionType, extractedUUID, transactionID := extractMessageTypeAndId(validMessage.Headers)
	assert := assert.New(t)
	assert.Equal("tid_newid", transactionID)
	assert.Equal("organisations", extractedIngestionType)
	assert.Equal("5e0ad5e5-c3d4-387d-9875-ec15501808e5", extractedUUID)
}

func TestWriterUrlIsResolvedCorrectlyAndRequestIsNotNull(t *testing.T) {
	validMessage := createMessage(uuid, validMessageTypeOrganisations)
	writerUrl, err := resolveWriter(validMessageTypeOrganisations, correctWriterMappings)
	request, reqURL, err := createWriteRequest(validMessageTypeOrganisations, strings.NewReader(validMessage.Body), uuid, writerUrl)
	assert := assert.New(t)
	assert.NoError(err)
	assert.Equal("http://localhost:8080/__organisations-rw-neo4j-blue/organisations/5e0ad5e5-c3d4-387d-9875-ec15501808e5", reqURL)
	assert.NotNil(request, "Request is nil")
}

func TestErrorIsThrownWhenIngestionTypeMatchesNoWriters(t *testing.T) {
	writerUrl, err := resolveWriter(invalidMessageType, correctWriterMappings)
	assert := assert.New(t)
	assert.Equal("", writerUrl)
	assert.Error(err, "No configured writer for concept: "+invalidMessageType)
}

func createMessage(messageID string, messageType string) queueConsumer.Message {
	return queueConsumer.Message{
		Headers: map[string]string{
			"Content-Type":      "application/json",
			"Message-Id":        messageID,
			"Message-Timestamp": "2016-06-16T08:14:36.910Z",
			"Message-Type":      messageType,
			"Origin-System-Id":  "http://cmdb.ft.com/systems/upp",
			"X-Request-Id":      "tid_newid",
		},
		Body: `{transformed-org-json`}
}
