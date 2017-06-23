package main

import (
	"fmt"
	"net/http"

	fthealth "github.com/Financial-Times/go-fthealth/v1_1"
	"github.com/Financial-Times/message-queue-gonsumer/consumer"
	"github.com/Financial-Times/service-status-go/gtg"
)

type HealthCheck struct {
	baseURLs          []string
	elasticsearchConf *ElasticsearchWriterConfig
	consumer          consumer.MessageConsumer
	client            *http.Client
}

type ElasticsearchWriterConfig struct {
	elasticsearchWriterUrl     string
	includeElasticsearchWriter bool
}

func NewHealthCheck(c consumer.MessageConsumer, baseURLs []string, elasticsearchConf *ElasticsearchWriterConfig, client *http.Client) *HealthCheck {
	return &HealthCheck{
		consumer:          c,
		baseURLs:          baseURLs,
		elasticsearchConf: elasticsearchConf,
		client:            client,
	}
}

func (h *HealthCheck) Health() func(w http.ResponseWriter, r *http.Request) {
	checks := []fthealth.Check{h.queueHealthCheck(), h.writerHealthCheck()}
	if h.elasticsearchConf.includeElasticsearchWriter {
		checks = append(checks, h.elasticHealthCheck())
	}
	hc := fthealth.HealthCheck{
		SystemCode:  "concept-ingester",
		Name:        "Concept Ingester",
		Description: "Consumes Concept instances and forwards them to respective database writers for each concept type.",
		Checks:      checks,
	}
	return fthealth.Handler(hc)
}

func (h *HealthCheck) queueHealthCheck() fthealth.Check {
	return fthealth.Check{
		Name:             "Message Queue Proxy Reachable",
		Severity:         1,
		BusinessImpact:   "Unable to connect to kafka proxy.",
		TechnicalSummary: "Cannot connect to kafka-proxy. If this check fails, check that cluster is up and running, proxy is healthy.",
		PanicGuide:       "https://dewey.ft.com/concept-ingester.html",
		Checker:          h.consumer.ConnectivityCheck,
	}
}

func (h *HealthCheck) writerHealthCheck() fthealth.Check {
	return fthealth.Check{
		BusinessImpact:   "Unable to connect to one or more configured writers",
		Name:             "Check connectivity to writers which are a parameter in hieradata for this service",
		PanicGuide:       "https://dewey.ft.com/concept-ingester.html",
		Severity:         1,
		TechnicalSummary: `Cannot connect to one or more configured writers. If this check fails, check that cluster is up and running and each configured writer returns a healthy gtg`,
		Checker:          h.checkCanConnectToWriters,
	}
}

func (h *HealthCheck) elasticHealthCheck() fthealth.Check {
	return fthealth.Check{
		BusinessImpact:   "Unable to connect to  elasticsearch concept writer",
		Name:             "Check connectivity to concept-rw-elasticsearch",
		PanicGuide:       "https://dewey.ft.com/concept-ingester.html",
		Severity:         1,
		TechnicalSummary: `Cannot connect to elasticsearch concept writer. If this check fails, check that the configured writer returns a healthy gtg`,
		Checker:          h.checkCanConnectToElasticsearchWriter,
	}
}

func (h *HealthCheck) GTG() gtg.Status {
	checks := make([]gtg.StatusChecker, 0)

	consumerCheck := func() gtg.Status {
		return gtgCheck(h.consumer.ConnectivityCheck)
	}
	writersCheck := func() gtg.Status {
		return gtgCheck(h.checkCanConnectToWriters)
	}

	checks = append(checks, consumerCheck, writersCheck)

	if h.elasticsearchConf.includeElasticsearchWriter {
		elasticsearchCheck := func() gtg.Status {
			return gtgCheck(h.checkCanConnectToElasticsearchWriter)
		}
		checks = append(checks, elasticsearchCheck)
	}

	return gtg.FailFastParallelCheck(checks)()
}

func (h *HealthCheck) checkCanConnectToWriters() (string, error) {
	err := h.checkWritersAvailability()
	if err != nil {
		return fmt.Sprintf("Healthcheck: Writer not available: %v", err.Error()), err
	}
	return "", nil
}

func (h *HealthCheck) checkCanConnectToElasticsearchWriter() (string, error) {
	err := h.checkWriterAvailability(h.elasticsearchConf.elasticsearchWriterUrl)
	if err != nil {
		return fmt.Sprintf("Healthcheck: Elasticsearch Concept Writer not available: %v", err.Error()), err
	}
	return "", nil
}

func (h *HealthCheck) checkWritersAvailability() error {
	for _, baseURL := range h.baseURLs {
		err := h.checkWriterAvailability(baseURL)
		if err != nil {
			return err
		}
	}
	return nil
}

func (h *HealthCheck) checkWriterAvailability(baseURL string) error {
	resp, err := h.client.Get(baseURL + "/__gtg")
	if err != nil {
		return fmt.Errorf("Error calling writer at %s : %v", baseURL+"/__gtg", err)
	}
	resp.Body.Close()
	if resp != nil && resp.StatusCode != http.StatusOK {
		return fmt.Errorf("Writer %v returned status %d", baseURL+"/__gtg", resp.StatusCode)
	}
	return nil
}

func gtgCheck(handler func() (string, error)) gtg.Status {
	if _, err := handler(); err != nil {
		return gtg.Status{GoodToGo: false, Message: err.Error()}
	}
	return gtg.Status{GoodToGo: true}
}
