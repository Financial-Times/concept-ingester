package main

import (
	"net/http"
	"os"

	queueConsumer "github.com/Financial-Times/message-queue-gonsumer/consumer"
	"strings"

	"errors"
	"github.com/Financial-Times/go-fthealth/v1a"
	"github.com/Financial-Times/http-handlers-go/httphandlers"
	status "github.com/Financial-Times/service-status-go/httphandlers"
	log "github.com/Sirupsen/logrus"
	"github.com/gorilla/mux"
	"github.com/jawher/mow.cli"
	"github.com/rcrowley/go-metrics"
	"io"
	"io/ioutil"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"
)

func main() {
	log.SetLevel(log.InfoLevel)
	app := cli.App("concept-ingester", "A microservice that consumes concept messages from Kafka and routes them to the appropriate writer")

	services := app.String(cli.StringOpt{
		Name:   "services-list",
		Value:  "services",
		Desc:   "writer services",
		EnvVar: "SERVICES",
	})
	port := app.String(cli.StringOpt{
		Name:   "port",
		Value:  "8080",
		Desc:   "Port to listen on",
		EnvVar: "PORT",
	})
	clientTimeout := app.Int(cli.IntOpt{
		Name:   "timeout",
		Value:  10,
		Desc:   "default timeout for connection to client",
		EnvVar: "TIMEOUT",
	})
	vulcanAddr := app.String(cli.StringOpt{
		Name:   "vulcan_addr",
		Value:  "https://vulcan-address",
		Desc:   "Vulcan address for routing requests",
		EnvVar: "VULCAN_ADDR",
	})
	consumerGroupID := app.String(cli.StringOpt{
		Name:   "consumer_group_id",
		Value:  "ConceptIngesterGroup",
		Desc:   "Kafka group id used for message consuming.",
		EnvVar: "GROUP_ID",
	})
	consumerQueue := app.String(cli.StringOpt{
		Name:   "consumer_queue_id",
		Value:  "",
		Desc:   "Sets host header",
		EnvVar: "HOST_HEADER",
	})
	consumerOffset := app.String(cli.StringOpt{
		Name:   "consumer_offset",
		Value:  "",
		Desc:   "Kafka read offset.",
		EnvVar: "OFFSET"})
	consumerAutoCommitEnable := app.Bool(cli.BoolOpt{
		Name:   "consumer_autocommit_enable",
		Value:  true,
		Desc:   "Enable autocommit for small messages.",
		EnvVar: "COMMIT_ENABLE"})
	consumerStreamCount := app.Int(cli.IntOpt{
		Name:   "consumer_stream_count",
		Value:  10,
		Desc:   "Number of consumer streams",
		EnvVar: "STREAM_COUNT"})
	topic := app.String(cli.StringOpt{
		Name:   "topic",
		Value:  "kafka-topic",
		Desc:   "Kafka topic subscribed to",
		EnvVar: "TOPIC"})

	//TODO can we use custom headers
	messageTypeEndpointsMap := map[string]string{
		"organisation": "organisations",
		"person":       "people",
		"membership":   "memberships",
		"role":         "roles",
		"brand":        "brands",
		"subject":      "subjects",
		"topic":        "topics",
		"section":      "sections",
		"genre":        "genre",
		"location":     "locations",
	}

	app.Action = func() {
		consumerConfig := queueConsumer.QueueConfig{}
		consumerConfig.Addrs = strings.Split(*vulcanAddr, ",")
		consumerConfig.Group = *consumerGroupID
		consumerConfig.Queue = *consumerQueue
		consumerConfig.Topic = *topic
		consumerConfig.Offset = *consumerOffset
		consumerConfig.StreamCount = *consumerStreamCount
		consumerConfig.AutoCommitEnable = *consumerAutoCommitEnable

		servicesMap := createServicesMap(*services, messageTypeEndpointsMap, *vulcanAddr)
		httpConfigurations := httpConfigurations{baseURLMap: servicesMap}
		log.Infof("concept-ingester-go-app will listen on port: %s", *port)

		client := http.Client{Timeout: time.Duration(time.Duration(*clientTimeout) * time.Second),
			Transport: &http.Transport{DisableKeepAlives: false, MaxIdleConnsPerHost: 32}}

		httpConfigurations.client = client
		consumer := queueConsumer.NewConsumer(consumerConfig, httpConfigurations.readMessage, client)

		var wg sync.WaitGroup
		wg.Add(1)

		go func() {
			consumer.Start()
			wg.Done()
		}()

		go runServer(httpConfigurations.baseURLMap, *port)

		ch := make(chan os.Signal)
		signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)

		<-ch
		log.Println("Shutting down application...")

		consumer.Stop()
		wg.Wait()

		log.Println("Application closing")
	}
	log.Infof("Application started with args %s", os.Args)
	app.Run(os.Args)
}

func createServicesMap(services string, messageTypeMap map[string]string, vulcanAddr string) map[string]string {
	stringSlice := strings.Split(services, ",")
	servicesMap := make(map[string]string)
	for _, service := range stringSlice {
		for messageType, concept := range messageTypeMap {
			if strings.Contains(service, concept) {
				writerURL := vulcanAddr + "/__" + service + "/" + concept
				if servicesMap[messageType] != nil {
					servicesMap[messageType] = writerURL
				} else {
					log.Errorf("Invalid message type %v", messageType)
				}
			}
		}
	}
	return servicesMap
}

func runServer(baseURLMap map[string]string, port string) {

	httpHandlers := httpHandlers{baseURLMap}

	r := router(httpHandlers)
	// The following endpoints should not be monitored or logged (varnish calls one of these every second, depending on config)
	// The top one of these build info endpoints feels more correct, but the lower one matches what we have in Dropwizard,
	// so it's what apps expect currently same as ping, the content of build-info needs more definition
	http.HandleFunc(status.PingPath, status.PingHandler)
	http.HandleFunc(status.PingPathDW, status.PingHandler)
	http.HandleFunc(status.BuildInfoPath, status.BuildInfoHandler)
	http.HandleFunc(status.BuildInfoPathDW, status.BuildInfoHandler)
	http.HandleFunc("/__gtg", httpHandlers.goodToGo)

	http.Handle("/", r)

	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("Unable to start server: %v\n", err)
	}
}

func router(hh httpHandlers) http.Handler {
	servicesRouter := mux.NewRouter()

	//TODO dont know how to do gtg
	//gtgChecker := make([]gtg.StatusChecker, 0)

	servicesRouter.HandleFunc("/__health", v1a.Handler("ConceptIngester Healthchecks",
		"Checks for accessing writer", hh.healthCheck()))

	servicesRouter.HandleFunc("/__gtg", hh.goodToGo)

	//TODO check writers /__health endpoint?
	//gtgChecker = append(gtgChecker, func() gtg.Status {
	//	if err := eng.Check(); err != nil {
	//		return gtg.Status{GoodToGo: false, Message: err.Error()}
	//	}
	//
	//	return gtg.Status{GoodToGo: true}
	//})

	var monitoringRouter http.Handler = servicesRouter
	monitoringRouter = httphandlers.TransactionAwareRequestLoggingHandler(log.StandardLogger(), monitoringRouter)
	monitoringRouter = httphandlers.HTTPMetricsHandler(metrics.DefaultRegistry, monitoringRouter)

	return monitoringRouter
}

type httpConfigurations struct {
	baseURLMap map[string]string
	client     http.Client
}

func (httpConf httpConfigurations) readMessage(msg queueConsumer.Message) {
	var ingestionType string
	var uuid string
	for k, v := range msg.Headers {
		if k == "Message-Type" {
			ingestionType = v
		}
		if k == "Message-Id" {
			uuid = v
		}
	}
	reqURL, err := sendToWriter(ingestionType, strings.NewReader(msg.Body), uuid, httpConf.baseURLMap, httpConf.client)

	if err != nil {
		log.Errorf("Error processing msg: %v with error %v to %v", msg, err, reqURL)
	}
}

func sendToWriter(ingestionType string, msgBody *strings.Reader, uuid string, urlMap map[string]string, client http.Client) (reqURL string, err error) {
	writerURL := urlMap[ingestionType]
	reqURL = writerURL + "/" + uuid

	request, err := http.NewRequest("PUT", reqURL, msgBody)
	request.ContentLength = -1
	resp, err := client.Do(request)

	defer func() {
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
	}()

	if resp.StatusCode == http.StatusOK {
		return reqURL, err
	}
	err = errors.New("Concept not written to " + reqURL + "! Status code was " + strconv.Itoa(resp.StatusCode))
	return reqURL, err
}
