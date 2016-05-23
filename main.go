package main

import (
	"net/http"
	"os"

	"fmt"
	queueConsumer "github.com/Financial-Times/message-queue-gonsumer/consumer"
	"strings"

	"github.com/Financial-Times/go-fthealth/v1a"
	"github.com/Financial-Times/http-handlers-go/httphandlers"
	status "github.com/Financial-Times/service-status-go/httphandlers"
	log "github.com/Sirupsen/logrus"
	"github.com/gorilla/mux"
	"github.com/jawher/mow.cli"
	"github.com/rcrowley/go-metrics"
	"os/signal"
	"sync"
	"syscall"
)

func main() {
	log.SetLevel(log.InfoLevel)
	log.Printf("Application started with args %s", os.Args)
	app := cli.App("concept-ingester", "A microservice that consumes concept messages from Kafka and routes them to the appropriate writer")
	services := app.StringOpt("services-list", "__organisations-rw-neo4j-blue,people-rw-neo4j-blue", "writer services")
	port := app.StringOpt("port", "8080", "Port to listen on")
	env := app.StringOpt("env", "semantic-up.ft.com", "environment this app is running in")

	consumerAddrs := app.StringOpt("consumer_proxy_addr", "https://proxy-address", "Comma separated kafka proxy hosts for message consuming.")
	consumerGroupID := app.StringOpt("consumer_group_id", "idiConcept", "Kafka group id used for message consuming.")
	consumerOffset := app.StringOpt("consumer_offset", "", "Kafka read offset.")
	consumerAutoCommitEnable := app.BoolOpt("consumer_autocommit_enable", false, "Enable autocommit for small messages.")

	topic := app.StringOpt("topic", "Concept", "Kafka topic subscribed to")

	consumerConfig := queueConsumer.QueueConfig{}
	consumerConfig.Addrs = strings.Split(*consumerAddrs, ",")
	consumerConfig.Group = *consumerGroupID
	consumerConfig.Topic = *topic
	consumerConfig.Offset = *consumerOffset
	consumerConfig.AutoCommitEnable = *consumerAutoCommitEnable

	//TODO can we use custom headers
	messageTypeEndpointsMap := map[string]string {
		"organisationIngestion":"organisations",
		"peopleIngestion":"people",
		"membershipIngestion":"memberships",
		"roleIngestion":"roles",
		"brandIngestion":"brands",
		"subjectIngestion":"subjects",
		"topicIngestion":"topics",
		"sectionIngestion":"sections",
		"genreIngestion":"genre",
		"locationIngestion":"locations",
	}

	servicesMap := createServicesMap(*services, messageTypeEndpointsMap, *env)

	// Could take the header to router
	httpConfigurations := httpConfigurations{servicesMap}

	app.Action = func() {
		if *env != "semantic-up.ft.com"  {
			f, err := os.OpenFile("/var/log/apps/concept-ingester-go-app.log", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0755)
			if err == nil {
				log.SetOutput(f)
				log.SetFormatter(&log.TextFormatter{DisableColors: true})
			} else {
				log.Fatalf("Failed to initialise log file, %v", err)
			}

			defer f.Close()
		}

		log.Infof("concept-ingester-go-app will listen on port: %s, connecting to: %s", *port)
		runServer(httpConfigurations.baseUrlMap, *port, *env)
	}

	consumer := queueConsumer.NewConsumer(consumerConfig, httpConfigurations.readMessage, http.Client{})
	go consumeKafkaMessages(consumer)

	log.SetLevel(log.InfoLevel)
	log.Infof("Application started with args %s", os.Args)
	app.Run(os.Args)
}

func createServicesMap(services string, messageTypeMap map[string]string, env string) (map[string]string){
	stringSlice := strings.Split(services, ",")
	servicesMap := make(map[string]string)
	for _, service := range stringSlice {
		for messageType, concept := range messageTypeMap {
			if strings.Contains(service, concept) {
				baseUrl := "https://" + env + "/" + service
				servicesMap[messageType] = baseUrl
				fmt.Printf("Added url %v to map:", baseUrl)
			}
		}
	}
	return servicesMap
}

func runServer(baseUrlMap map[string]string, port string, env string) {

	httpHandlers := httpHandlers{baseUrlMap}

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
		log.Fatalf("Unable to start server: %v", err)
	}
}

func router(hh httpHandlers) http.Handler {
	servicesRouter := mux.NewRouter()

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

	// Then API specific ones:
	//TODO: What endpoints do we want?
	servicesRouter.HandleFunc("/content/{uuid}/annotations", hh.methodNotAllowedHandler)

	var monitoringRouter http.Handler = servicesRouter
	monitoringRouter = httphandlers.TransactionAwareRequestLoggingHandler(log.StandardLogger(), monitoringRouter)
	monitoringRouter = httphandlers.HTTPMetricsHandler(metrics.DefaultRegistry, monitoringRouter)

	return monitoringRouter
}

func consumeKafkaMessages(consumer queueConsumer.Consumer) {

	log.Print("Splat!")
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		consumer.Start()
		wg.Done()
	}()

	log.Print("Thwomp!")
	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch


	log.Print("Biff!")
	consumer.Stop()
	wg.Wait()
}

type httpConfigurations struct {
	baseUrlMap map[string]string
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
	fmt.Printf("Ingestion Type is %v", ingestionType)
	_, err, writerUrl := sendToWriter(ingestionType, strings.NewReader(msg.Body), uuid, httpConf.baseUrlMap)

	if err == nil {
		log.Infof("Successfully written msg: %v to writer: %v", msg, writerUrl)
	} else {
		log.Errorf("Error processing msg: %s", msg)
	}
}

func sendToWriter(ingestionType string, msgBody *strings.Reader, uuid string, urlMap map[string]string) (resp *http.Response, err error, writerUrl string) {
	for messageType, baseUrl := range urlMap {
		if messageType == ingestionType {
			writerUrl = resolveEndpoint(ingestionType, baseUrl)
		}
	}
	fullUrl := writerUrl + "/" + uuid

	client := &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 50,},}
	request, err := http.NewRequest("PUT", fullUrl, msgBody)
	request.ContentLength = -1
	resp, err = client.Do(request)
	return resp, err, writerUrl
}

func resolveEndpoint(ingestionType string, baseUrl string) (writerUrl string) {
	switch ingestionType {
	case "peopleIngestion":
		return baseUrl + "/people"
	case "organisationIngestion":
		return baseUrl + "/organisations"
	case "membershipIngestion":
		return baseUrl + "/memberships"
	case "roleIngestion":
		return baseUrl + "/roles"
	case "brandIngestion":
		return baseUrl + "/brands"
	case "subjectIngestion":
		return baseUrl + "/subjectss"
	case "topicIngestion":
		return baseUrl + "/topics"
	case "sectionIngestion":
		return baseUrl + "/sections"
	case "genreIngestion":
		return baseUrl + "/genres"
	case "locationIngestion":
		return baseUrl + "/locations"
	default:
		return "unknown message type"
	}
}