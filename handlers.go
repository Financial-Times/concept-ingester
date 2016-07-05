package main

import (
	"fmt"
	"net/http"

	"github.com/Financial-Times/go-fthealth/v1a"
	"regexp"
	log "github.com/Sirupsen/logrus"
	"io"
	"io/ioutil"
	"encoding/json"
)

type httpHandlers struct {
	baseURLMap map[string]string
	vulcanAddr string
	topic string
}

func (hh *httpHandlers) healthCheck() v1a.Check {
	return v1a.Check{
		BusinessImpact:   "Unable to connect to kakfa proxy",
		Name:             "Check connectivity to kafka-proxy and presence of configured topic which is a parameter in hieradata for this service",
		PanicGuide:       "TODO",
		Severity:         1,
		TechnicalSummary: `Cannot connect to kafka-proxy or configured topic is not present. If this check fails, check that cluster is up and running, proxy is healthy and configured topic is present on the queue.`,
		Checker:          hh.checkCanConnectToProxy,
	}
}

//TODO checks availability of writers not connectivity to Kafka?

func (hh *httpHandlers) checkCanConnectToProxy() (string, error) {
	body, err := checkProxyConnection(hh.vulcanAddr)
	if err != nil {
		log.Errorf("Healthcheck: Error reading request body: %v", err.Error())
		return "", err
	}
	return checkIfTopicIsPresent(body, hh.topic)
}

func checkProxyConnection(vulcanAddr string) (body []byte, err error){
	//check if proxy is running and topic is present
	req, err := http.NewRequest("GET", vulcanAddr + "/topics", nil)
	if err != nil {
		log.Errorf("Creating kafka-proxy check resulted in error: %v", err.Error())
		return nil, err
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		log.Errorf("Healthcheck: Execution of kafka-proxy GET request resulted in error: %v", err.Error())
	}
	defer func() {
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
	}()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Connecting to kafka-proxy was unsuccessful. Status was %v", resp.StatusCode)
	}
	return ioutil.ReadAll(resp.Body)
}

func checkIfTopicIsPresent(body []byte, expectedTopic string) (string, error) {
	var registeredTopics []string
	err := json.Unmarshal(body, &registeredTopics)
	if err != nil {
		return "", fmt.Errorf("Connection established to kafka-proxy, but parsing response resulted in following error: ", err.Error())
	}

	for _, topic := range registeredTopics {
		if topic == expectedTopic {
			return "", nil
		}
	}
	return "", fmt.Errorf("Connection established to kafka-proxy, but expected topic %v was not found", expectedTopic)
}

func (hh *httpHandlers) ping(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "pong")
}

//goodToGo returns a 503 if the healthcheck fails - suitable for use from varnish to check availability of a node
func (hh *httpHandlers) goodToGo(writer http.ResponseWriter, req *http.Request) {
	if err := hh.checkWriterAvailability(); err != nil {
	}
}

func (hh *httpHandlers) checkWriterAvailability() error {
	var endpointsToCheck []string
	for _, baseURL := range hh.baseURLMap {
		reg, _ := regexp.Compile("\\w*$")
		endpointsToCheck = append(endpointsToCheck, reg.ReplaceAllLiteralString(baseURL, "__gtg"))
	}
	goodToGo, gtgErr := checkWriterStatus(endpointsToCheck)
	if goodToGo == false {
		return gtgErr
	}
	return nil
}

func checkWriterStatus(endpointsToCheck []string) (bool, error) {
	for _, writerG2G := range endpointsToCheck {
		fmt.Printf("Writer g2g: %v \n", writerG2G)
		resp, err := http.Get(writerG2G)
		if err != nil || resp.StatusCode != http.StatusOK {
			return false, err
		}
	}
	return true, nil
}

// buildInfoHandler - This is a stop gap and will be added to when we can define what we should display here
func (hh *httpHandlers) buildInfoHandler(w http.ResponseWriter, req *http.Request) {
	fmt.Fprintf(w, "build-info")
}
