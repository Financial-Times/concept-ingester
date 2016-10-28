package main

import (
	"fmt"
	"net/http"

	"io"
	"io/ioutil"

	"github.com/Financial-Times/go-fthealth/v1a"
	log "github.com/Sirupsen/logrus"
)

type httpHandlers struct {
	baseURLMappings map[string]string
	vulcanAddr      string
	topic           string
}

func (hh *httpHandlers) kafkaProxyHealthCheck() v1a.Check {
	return v1a.Check{
		BusinessImpact:   "Unable to connect to kafka proxy",
		Name:             "Check connectivity to kafka-proxy and presence of configured topic which is a parameter in hieradata for this service",
		PanicGuide:       "https://sites.google.com/a/ft.com/universal-publishing/ops-guides/concept-ingestion",
		Severity:         1,
		TechnicalSummary: `Cannot connect to kafka-proxy. If this check fails, check that cluster is up and running, proxy is healthy and configured topic is present on the queue.`,
		Checker:          hh.checkCanConnectToKafkaProxy,
	}
}

func (hh *httpHandlers) writerHealthCheck() v1a.Check {
	return v1a.Check{
		BusinessImpact:   "Unable to connect to one or more configured writers",
		Name:             "Check connectivity to writers which are a parameter in hieradata for this service",
		PanicGuide:       "https://sites.google.com/a/ft.com/universal-publishing/ops-guides/concept-ingestion",
		Severity:         1,
		TechnicalSummary: `Cannot connect to one or more configured writers. If this check fails, check that cluster is up and running and each configured writer returns a healthy gtg`,
		Checker:          hh.checkCanConnectToWriters,
	}
}

func (hh *httpHandlers) checkCanConnectToKafkaProxy() (string, error) {
	_, err := checkProxyConnection(hh.vulcanAddr)
	if err != nil {
		return fmt.Sprintf("Healthcheck: Error reading request body: %v", err.Error()), err
	}
	return "", nil
}

func (hh *httpHandlers) checkCanConnectToWriters() (string, error) {
	err := checkWriterAvailability(hh.baseURLMappings)
	if err != nil {
		return fmt.Sprintf("Healthcheck: Writer not available: %v", err.Error()), err
	}
	return "", nil
}

func checkProxyConnection(vulcanAddr string) (body []byte, err error) {
	//check if proxy is running and topic is present
	req, err := http.NewRequest("GET", vulcanAddr+"/topics", nil)
	if err != nil {
		log.Errorf("Creating kafka-proxy check resulted in error: %v", err.Error())
		return nil, err
	}
	req.Host = "kafka"
	resp, err := httpClient.Do(req)
	if err != nil {
		log.Errorf("Healthcheck: Execution of kafka-proxy GET request resulted in error: %v", err.Error())
	}
	defer func() {
		if resp == nil {
			return
		}
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
	}()
	if resp == nil {
		return nil, fmt.Errorf("Connecting to kafka-proxy was unsuccessful.")
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Connecting to kafka-proxy was unsuccessful. Status was %v", resp.StatusCode)
	}
	return ioutil.ReadAll(resp.Body)
}

func (hh *httpHandlers) ping(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "pong")
}

//goodToGo returns a 503 if the healthcheck fails - suitable for use from varnish to check availability of a node
func (hh *httpHandlers) goodToGo(writer http.ResponseWriter, req *http.Request) {
	if _, err := hh.checkCanConnectToKafkaProxy(); err != nil {
		writer.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	if _, err := hh.checkCanConnectToWriters(); err != nil {
		writer.WriteHeader(http.StatusServiceUnavailable)
		return
	}
}

func checkWriterAvailability(baseURLMapping map[string]string) error {
	for _, baseURL := range baseURLMapping {
		resp, err := http.Get(baseURL + "/__gtg")
		if err != nil {
			return fmt.Errorf("Error calling writer at %s : %v", baseURL+"/__gtg", err)
		}
		resp.Body.Close()
		if resp != nil && resp.StatusCode != http.StatusOK {
			return fmt.Errorf("Writer %v returned status %d", baseURL+"/__gtg", resp.StatusCode)
		}
	}
	return nil
}

// buildInfoHandler - This is a stop gap and will be added to when we can define what we should display here
func (hh *httpHandlers) buildInfoHandler(w http.ResponseWriter, req *http.Request) {
	fmt.Fprintf(w, "build-info")
}
