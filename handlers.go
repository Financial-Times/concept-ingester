package main

import (
	"fmt"
	"net/http"

	"github.com/Financial-Times/go-fthealth/v1a"
	"regexp"
)

type httpHandlers struct {
	baseUrlMap map[string]string
}

func (hh *httpHandlers) healthCheck() v1a.Check {
	return v1a.Check{
		BusinessImpact:   "Unable to write concepts to one or more of the configured writers",
		Name:             "Check connectivity to neo rw services - services is a parameter in hieradata for this service",
		PanicGuide:       "TODO",
		Severity:         1,
		TechnicalSummary: `Cannot connect to Neo4j rw services. If this check fails, check that cluster is up and running and configured rw's are healthy. You can find the services configured as a parameter in hieradata for this service.`,
		Checker:          hh.checker,
	}
}

//TODO checks availability of writers not connectivity to Kafka?
func (hh *httpHandlers) checker() (string, error) {
	var endpointsToCheck []string
	for _, baseUrl := range hh.baseUrlMap {
		reg, _ := regexp.Compile("\\w*$")
		//g2g := reg.ReplaceAllLiteralString(baseUrl, "__gtg")
		endpointsToCheck = append(endpointsToCheck, reg.ReplaceAllLiteralString(baseUrl, "__gtg"))
	}
	for _, writerG2G := range endpointsToCheck {
		resp, err := http.Get(writerG2G)
		if err != nil {
			return "", err
		}
		if resp.StatusCode != http.StatusOK {
			return resp.Status, nil
		}
	}

	return "All Writers are good to go", nil
}

func (hh *httpHandlers) ping(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "pong")
}

//goodToGo returns a 503 if the healthcheck fails - suitable for use from varnish to check availability of a node
func (hh *httpHandlers) goodToGo(writer http.ResponseWriter, req *http.Request) {
	if _, err := hh.checker(); err != nil {
		writer.WriteHeader(http.StatusServiceUnavailable)
	}

}

// buildInfoHandler - This is a stop gap and will be added to when we can define what we should display here
func (hh *httpHandlers) buildInfoHandler(w http.ResponseWriter, req *http.Request) {
	fmt.Fprintf(w, "build-info")
}

// methodNotAllowedHandler handles 405
func (hh *httpHandlers) methodNotAllowedHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusMethodNotAllowed)
	return
}
