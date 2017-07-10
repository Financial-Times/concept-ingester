package main

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Financial-Times/message-queue-gonsumer/consumer"
	"github.com/stretchr/testify/assert"
)

func TestNewHealthCheck(t *testing.T) {
	hc := NewHealthCheck(
		consumer.NewConsumer(consumer.QueueConfig{}, func(m consumer.Message) {}, http.DefaultClient),
		[]string{"baseURLMappings"},
		&ElasticsearchWriterConfig{
			elasticsearchWriterUrl:     "elasticsearchWriterUrl",
			includeElasticsearchWriter: true,
		},
		http.DefaultClient,
	)

	assert.NotNil(t, hc.consumer)
	assert.NotNil(t, hc.elasticsearchConf)
	assert.True(t, hc.elasticsearchConf.includeElasticsearchWriter)
	assert.Equal(t, hc.elasticsearchConf.elasticsearchWriterUrl, "elasticsearchWriterUrl")
	assert.NotNil(t, hc.client)
	assert.Contains(t, hc.baseURLs, "baseURLMappings")
}

func TestHappyHealthCheckWithElasticsearchWriter(t *testing.T) {
	endpoints := []endpoint{
		{
			relativeURL: "/first",
			status:      http.StatusOK,
		},
		{
			relativeURL: "/second",
			status:      http.StatusOK,
		},
		{
			relativeURL: "/elasticsearch",
			status:      http.StatusOK,
		},
	}
	server := getMockedServer(endpoints)
	defer server.Close()
	baseURLs := getFullURLs(endpoints, server.URL)
	hc := initHealthCheckWithElasticsearchWriter(true, baseURLs[:2], baseURLs[2])

	req := httptest.NewRequest("GET", "http://example.com/__health", nil)
	w := httptest.NewRecorder()

	hc.Health()(w, req)

	assert.Equal(t, 200, w.Code, "It should return HTTP 200 OK")
	assert.Contains(t, w.Body.String(), `"name":"Message Queue Proxy Reachable","ok":true`, "Message queue proxy healthcheck should be happy")
	assert.Contains(t, w.Body.String(), `"name":"Check connectivity to writers","ok":true`, "Writers healthcheck should be happy")
	assert.Contains(t, w.Body.String(), `"name":"Check connectivity to concept-rw-elasticsearch","ok":true`, "Concept-rw-elasticsearch healthcheck should be happy")
}

func TestHappyHealthCheckWithoutElasticsearchWriter(t *testing.T) {
	endpoints := []endpoint{
		{
			relativeURL: "/first",
			status:      http.StatusOK,
		},
		{
			relativeURL: "/second",
			status:      http.StatusOK,
		},
	}
	server := getMockedServer(endpoints)
	baseURLs := getFullURLs(endpoints, server.URL)
	hc := initHealthCheckWithoutElasticsearchWriter(true, baseURLs)

	req := httptest.NewRequest("GET", "http://example.com/__health", nil)
	w := httptest.NewRecorder()

	hc.Health()(w, req)

	assert.Equal(t, 200, w.Code, "It should return HTTP 200 OK")
	assert.Contains(t, w.Body.String(), `"name":"Message Queue Proxy Reachable","ok":true`, "Message queue proxy healthcheck should be happy")
	assert.Contains(t, w.Body.String(), `"name":"Check connectivity to writers","ok":true`, "Writers healthcheck should be happy")
	assert.NotContains(t, w.Body.String(), `"name":"Check connectivity to concept-rw-elasticsearch`, "The connectivity to the concept-rw-elasticsearch should not be checked")
}

func TestHealthCheckWithUnhappyConsumerWithElasticsearchWriter(t *testing.T) {
	endpoints := []endpoint{
		{
			relativeURL: "/first",
			status:      http.StatusOK,
		},
		{
			relativeURL: "/second",
			status:      http.StatusOK,
		},
		{
			relativeURL: "/elasticsearch",
			status:      http.StatusOK,
		},
	}
	server := getMockedServer(endpoints)
	baseURLs := getFullURLs(endpoints, server.URL)
	hc := initHealthCheckWithElasticsearchWriter(false, baseURLs[:2], baseURLs[2])

	req := httptest.NewRequest("GET", "http://example.com/__health", nil)
	w := httptest.NewRecorder()

	hc.Health()(w, req)

	assert.Equal(t, 200, w.Code, "It should return HTTP 200 OK")
	assert.Contains(t, w.Body.String(), `"name":"Message Queue Proxy Reachable","ok":false`, "Message queue proxy healthcheck should be unhappy")
	assert.Contains(t, w.Body.String(), `"name":"Check connectivity to writers which are a parameter in hieradata for this service","ok":true`, "Writers healthcheck should be happy")
	assert.Contains(t, w.Body.String(), `"name":"Check connectivity to concept-rw-elasticsearch","ok":true`, "Concept-rw-elasticsearch healthcheck should be happy")
}

func TestHealthCheckWithUnhappyConsumerWithoutElasticsearchWriter(t *testing.T) {
	endpoints := []endpoint{
		{
			relativeURL: "/first",
			status:      http.StatusOK,
		},
		{
			relativeURL: "/second",
			status:      http.StatusOK,
		},
	}
	server := getMockedServer(endpoints)
	baseURLs := getFullURLs(endpoints, server.URL)
	hc := initHealthCheckWithoutElasticsearchWriter(false, baseURLs)

	req := httptest.NewRequest("GET", "http://example.com/__health", nil)
	w := httptest.NewRecorder()

	hc.Health()(w, req)

	assert.Equal(t, 200, w.Code, "It should return HTTP 200 OK")
	assert.Contains(t, w.Body.String(), `"name":"Message Queue Proxy Reachable","ok":false`, "Message queue proxy healthcheck should be unhappy")
	assert.Contains(t, w.Body.String(), `"name":"Check connectivity to writers","ok":true`, "Writers healthcheck should be happy")
	assert.NotContains(t, w.Body.String(), `"name":"Check connectivity to concept-rw-elasticsearch`, "The connectivity to the concept-rw-elasticsearch should not be checked")
}

func TestHealthCheckWithUnhappyWriterAndWithElasticsearchWriter(t *testing.T) {
	endpoints := []endpoint{
		{
			relativeURL: "/first",
			status:      http.StatusOK,
		},
		{
			relativeURL: "/second",
			status:      http.StatusServiceUnavailable,
		},
		{
			relativeURL: "/elasticsearch",
			status:      http.StatusOK,
		},
	}
	server := getMockedServer(endpoints)
	baseURLs := getFullURLs(endpoints, server.URL)
	hc := initHealthCheckWithElasticsearchWriter(true, baseURLs[:2], baseURLs[2])

	req := httptest.NewRequest("GET", "http://example.com/__health", nil)
	w := httptest.NewRecorder()

	hc.Health()(w, req)

	assert.Equal(t, 200, w.Code, "It should return HTTP 200 OK")
	assert.Contains(t, w.Body.String(), `"name":"Message Queue Proxy Reachable","ok":true`, "Message queue proxy healthcheck should be happy")
	assert.Contains(t, w.Body.String(), `"name":"Check connectivity to writers","ok":false`, "Writers healthcheck should be unhappy")
	assert.Contains(t, w.Body.String(), `"name":"Check connectivity to concept-rw-elasticsearch","ok":true`, "Concept-rw-elasticsearch healthcheck should be happy")
}

func TestHealthCheckWithUnhappyWriterAndWithoutElasticsearchWriter(t *testing.T) {
	endpoints := []endpoint{
		{
			relativeURL: "/first",
			status:      http.StatusOK,
		},
		{
			relativeURL: "/second",
			status:      http.StatusServiceUnavailable,
		},
	}
	server := getMockedServer(endpoints)
	baseURLs := getFullURLs(endpoints, server.URL)
	hc := initHealthCheckWithoutElasticsearchWriter(true, baseURLs)

	req := httptest.NewRequest("GET", "http://example.com/__health", nil)
	w := httptest.NewRecorder()

	hc.Health()(w, req)

	assert.Equal(t, 200, w.Code, "It should return HTTP 200 OK")
	assert.Contains(t, w.Body.String(), `"name":"Message Queue Proxy Reachable","ok":true`, "Message queue proxy healthcheck should be happy")
	assert.Contains(t, w.Body.String(), `"name":"Check connectivity to writers","ok":false`, "Writers healthcheck should be unhappy")
	assert.NotContains(t, w.Body.String(), `"name":"Check connectivity to concept-rw-elasticsearch`, "The connectivity to the concept-rw-elasticsearch should not be checked")
}

func TestHealthCheckUnhappyElasticsearchWriter(t *testing.T) {
	endpoints := []endpoint{
		{
			relativeURL: "/first",
			status:      http.StatusOK,
		},
		{
			relativeURL: "/second",
			status:      http.StatusOK,
		},
		{
			relativeURL: "/elasticsearch",
			status:      http.StatusServiceUnavailable,
		},
	}
	server := getMockedServer(endpoints)
	baseURLs := getFullURLs(endpoints, server.URL)
	hc := initHealthCheckWithElasticsearchWriter(true, baseURLs[:2], baseURLs[2])

	req := httptest.NewRequest("GET", "http://example.com/__health", nil)
	w := httptest.NewRecorder()

	hc.Health()(w, req)

	assert.Equal(t, 200, w.Code, "It should return HTTP 200 OK")
	assert.Contains(t, w.Body.String(), `"name":"Message Queue Proxy Reachable","ok":true`, "Message queue proxy healthcheck should be happy")
	assert.Contains(t, w.Body.String(), `"name":"Check connectivity to writers","ok":true`, "Writers healthcheck should be happy")
	assert.Contains(t, w.Body.String(), `"name":"Check connectivity to concept-rw-elasticsearch","ok":false`, "Concept-rw-elasticsearch healthcheck should be unhappy")
}

func TestHappyGTGWithElasticsearchWriter(t *testing.T) {
	endpoints := []endpoint{
		{
			relativeURL: "/first",
			status:      http.StatusOK,
		},
		{
			relativeURL: "/second",
			status:      http.StatusOK,
		},
		{
			relativeURL: "/elasticsearch",
			status:      http.StatusOK,
		},
	}
	server := getMockedServer(endpoints)
	baseURLs := getFullURLs(endpoints, server.URL)
	hc := initHealthCheckWithElasticsearchWriter(true, baseURLs[:2], baseURLs[2])

	status := hc.GTG()
	assert.True(t, status.GoodToGo)
	assert.Empty(t, status.Message)
}

func TestHappyGTGWithoutElasticsearchWriter(t *testing.T) {
	endpoints := []endpoint{
		{
			relativeURL: "/first",
			status:      http.StatusOK,
		},
		{
			relativeURL: "/second",
			status:      http.StatusOK,
		},
	}
	server := getMockedServer(endpoints)
	baseURLs := getFullURLs(endpoints, server.URL)
	hc := initHealthCheckWithoutElasticsearchWriter(true, baseURLs)

	status := hc.GTG()
	assert.True(t, status.GoodToGo)
	assert.Empty(t, status.Message)
}

func TestGTGWithUnhappyConsumerWithElasticsearchWriter(t *testing.T) {
	endpoints := []endpoint{
		{
			relativeURL: "/first",
			status:      http.StatusOK,
		},
		{
			relativeURL: "/second",
			status:      http.StatusOK,
		},
		{
			relativeURL: "/elasticsearch",
			status:      http.StatusOK,
		},
	}
	server := getMockedServer(endpoints)
	baseURLs := getFullURLs(endpoints, server.URL)
	hc := initHealthCheckWithElasticsearchWriter(false, baseURLs[:2], baseURLs[2])

	status := hc.GTG()
	assert.False(t, status.GoodToGo)
	assert.Equal(t, "Error connecting to the queue", status.Message)
}

func TestGTGWithUnhappyConsumerWithoutElasticsearchWriter(t *testing.T) {
	endpoints := []endpoint{
		{
			relativeURL: "/first",
			status:      http.StatusOK,
		},
		{
			relativeURL: "/second",
			status:      http.StatusOK,
		},
	}
	server := getMockedServer(endpoints)
	baseURLs := getFullURLs(endpoints, server.URL)
	hc := initHealthCheckWithoutElasticsearchWriter(false, baseURLs)

	status := hc.GTG()
	assert.False(t, status.GoodToGo)
	assert.Equal(t, "Error connecting to the queue", status.Message)
}

func TestGTGWithUnhappyWriterWithElasticsearchWriter(t *testing.T) {
	endpoints := []endpoint{
		{
			relativeURL: "/first",
			status:      http.StatusOK,
		},
		{
			relativeURL: "/second",
			status:      http.StatusServiceUnavailable,
		},
		{
			relativeURL: "/elasticsearch",
			status:      http.StatusOK,
		},
	}
	server := getMockedServer(endpoints)
	baseURLs := getFullURLs(endpoints, server.URL)
	hc := initHealthCheckWithElasticsearchWriter(true, baseURLs[:2], baseURLs[2])

	status := hc.GTG()
	assert.False(t, status.GoodToGo)
	assert.Contains(t, status.Message, "/second/__gtg returned status 503")
}

func TestGTGWithUnhappyWriterWithoutElasticsearchWriter(t *testing.T) {
	endpoints := []endpoint{
		{
			relativeURL: "/first",
			status:      http.StatusOK,
		},
		{
			relativeURL: "/second",
			status:      http.StatusServiceUnavailable,
		},
	}
	server := getMockedServer(endpoints)
	baseURLs := getFullURLs(endpoints, server.URL)
	hc := initHealthCheckWithoutElasticsearchWriter(true, baseURLs)

	status := hc.GTG()
	assert.False(t, status.GoodToGo)
	assert.Contains(t, status.Message, "/second/__gtg returned status 503")
}

func TestGTGWithUnhappyElasticsearchWriter(t *testing.T) {
	endpoints := []endpoint{
		{
			relativeURL: "/first",
			status:      http.StatusOK,
		},
		{
			relativeURL: "/second",
			status:      http.StatusOK,
		},
		{
			relativeURL: "/elasticsearch",
			status:      http.StatusServiceUnavailable,
		},
	}
	server := getMockedServer(endpoints)
	baseURLs := getFullURLs(endpoints, server.URL)
	hc := initHealthCheckWithElasticsearchWriter(true, baseURLs[:2], baseURLs[2])

	status := hc.GTG()
	assert.False(t, status.GoodToGo)
	assert.Contains(t, status.Message, "/elasticsearch/__gtg returned status 503")
}

func getMockedServer(endpoints []endpoint) *httptest.Server {
	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	for _, endpoint := range endpoints {
		end := endpoint // this is needed because of function closures
		mux.HandleFunc(end.relativeURL+"/__gtg", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(end.status)
		})
	}
	return server
}

func getFullURLs(endpoints []endpoint, baseURL string) []string {
	URLs := make([]string, 0, len(endpoints))
	for _, e := range endpoints {
		URLs = append(URLs, baseURL+e.relativeURL)
	}
	return URLs
}

func initHealthCheckWithElasticsearchWriter(isConsumerConnectionHealthy bool, baseURLs []string, elasticsearchWriterUrl string) *HealthCheck {
	return &HealthCheck{
		consumer: &mockConsumerInstance{isConnectionHealthy: isConsumerConnectionHealthy},
		baseURLs: baseURLs,
		elasticsearchConf: &ElasticsearchWriterConfig{
			elasticsearchWriterUrl:     elasticsearchWriterUrl,
			includeElasticsearchWriter: true,
		},
		client: http.DefaultClient,
	}
}

func initHealthCheckWithoutElasticsearchWriter(isConsumerConnectionHealthy bool, baseURLs []string) *HealthCheck {
	return &HealthCheck{
		consumer: &mockConsumerInstance{isConnectionHealthy: isConsumerConnectionHealthy},
		baseURLs: baseURLs,
		elasticsearchConf: &ElasticsearchWriterConfig{
			includeElasticsearchWriter: false,
		},
		client: http.DefaultClient,
	}
}

type mockConsumerInstance struct {
	isConnectionHealthy bool
}

func (c *mockConsumerInstance) Start() {
}

func (c *mockConsumerInstance) Stop() {
}

func (c *mockConsumerInstance) ConnectivityCheck() (string, error) {
	if c.isConnectionHealthy {
		return "", nil
	}

	return "", errors.New("Error connecting to the queue")
}

type endpoint struct {
	relativeURL string
	status      int
}
