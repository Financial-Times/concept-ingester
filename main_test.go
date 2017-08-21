package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"fmt"

	queueConsumer "github.com/Financial-Times/message-queue-gonsumer/consumer"
	"github.com/rcrowley/go-metrics"
	"github.com/stretchr/testify/assert"
)

var peopleServiceVulcanRouting = "people-rw-neo4j"
var organisationsServiceVulcanRouting = "organisations-rw-neo4j"
var correctWriterMappingsVulcanRouting = map[string]string{
	"people-rw-neo4j":        "http://localhost:8080/__people-rw-neo4j",
	"organisations-rw-neo4j": "http://localhost:8080/__organisations-rw-neo4j",
}

var peopleService = "http://people-rw-neo4j:8080"
var organisationsService = "http://organisations-rw-neo4j:8080"
var correctWriterMappings = map[string]string{
	"http://people-rw-neo4j:8080":        "http://people-rw-neo4j:8080",
	"http://organisations-rw-neo4j:8080": "http://organisations-rw-neo4j:8080",
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
	assert := assert.New(t)
	tests := []struct {
		name                   string
		services               string
		expectedWriterMappings map[string]string
		vulcanRouting          bool
	}{
		{"Writer URLs parsed and composed correctly for clusters WITHOUT Vulcan routing",
			peopleService + "," + organisationsService,
			correctWriterMappings,
			false,
		},
		{
			"Writer URLs parsed and composed correctly for clusters WITH Vulcan routing",
			peopleServiceVulcanRouting + "," + organisationsServiceVulcanRouting,
			correctWriterMappingsVulcanRouting,
			true,
		},
	}

	for _, test := range tests {
		actualWriterMappings := createWriterMappings(test.services, "http://localhost:8080", test.vulcanRouting)
		assert.EqualValues(test.expectedWriterMappings, actualWriterMappings, fmt.Sprintf("%s: Service mappings incorrect.", test.name))
	}
}

func TestWriterServiceSliceCreationLocal(t *testing.T) {
	writerMappings := createWriterMappings(peopleServiceVulcanRouting+":7070,"+organisationsServiceVulcanRouting+":7080", "http://localhost:8080", true)
	localWriterMappings := map[string]string{
		"people-rw-neo4j:7070":        "http://localhost:7070",
		"organisations-rw-neo4j:7080": "http://localhost:7080",
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
	assert := assert.New(t)

	tests := []struct {
		name           string
		validMessage   queueConsumer.Message
		mappings       map[string]string
		expectedReqURL string
	}{
		{
			"Writer request is composed correctly in cluster WITH vulcan routing",
			createMessage(uuid, validMessageTypeOrganisations),
			correctWriterMappingsVulcanRouting,
			"http://localhost:8080/__organisations-rw-neo4j/organisations/5e0ad5e5-c3d4-387d-9875-ec15501808e5",
		},
		{
			"Writer request is composed correctly in cluster WITHOUT vulcan routing",
			createMessage(uuid, validMessageTypeOrganisations),
			correctWriterMappings,
			"http://organisations-rw-neo4j:8080/organisations/5e0ad5e5-c3d4-387d-9875-ec15501808e5",
		},
	}

	for _, test := range tests {
		writerUrl, err := resolveWriter(validMessageTypeOrganisations, test.mappings)
		request, actualReqURL, err := createWriteRequest(validMessageTypeOrganisations, strings.NewReader(test.validMessage.Body), uuid, writerUrl)
		assert.NoError(err, fmt.Sprintf("%s: Creating write request returns an error.", test.name))
		assert.Equal(test.expectedReqURL, actualReqURL, fmt.Sprintf("%s: Writer request URL is incorrect.", test.name))
		assert.NotNil(request, fmt.Sprintf("%s: Writer request is nil.", test.name))
	}
}

func TestErrorIsThrownWhenIngestionTypeMatchesNoWriters(t *testing.T) {
	assert := assert.New(t)
	tests := []struct {
		name     string
		mappings map[string]string
	}{
		{
			"Error is thrown when ingestion type matches no writers in a cluster WITHOUT vulcan routing",
			correctWriterMappings,
		},
		{
			"Error is thrown when ingestion type matches no writers in a cluster WITH vulcan routing",
			correctWriterMappingsVulcanRouting,
		},
	}

	for _, test := range tests {
		writerUrl, err := resolveWriter(invalidMessageType, test.mappings)
		assert.Equal("", writerUrl, fmt.Sprintf("%s: Writer URL is not empty.", test.name))
		assert.Error(err, "No configured writer for concept: "+invalidMessageType, fmt.Sprintf("%s: Error not returned from resolving writer.", test.name))
	}
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
