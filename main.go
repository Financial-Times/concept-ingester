package main

import (
	"net/http"
	"os"

	"fmt"
	queueConsumer "github.com/Financial-Times/message-queue-gonsumer/consumer"
	"strconv"
	"strings"
	"time"

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
	"io"
)

func main() {
	log.SetLevel(log.InfoLevel)
	log.Printf("Application started with args %s", os.Args)
	app := cli.App("concept-ingester", "A microservice that consumes concept messages from Kafka and routes them to the appropriate writer")
	services := app.StringOpt("services-list", "__organisations-rw-neo4j-blue,people-rw-neo4j-blue", "writer services")
	port := app.StringOpt("port", "8080", "Port to listen on")
	env := app.StringOpt("env", "semantic-up.ft.com", "environment this app is running in")
	cacheDuration := app.StringOpt("cache-duration", "30s", "Duration Get requests should be cached for. e.g. 2h45m would set the max-age value to '7440' seconds")

	consumerAddrs := app.StringOpt("consumer_proxy_addr", "https://kafka-proxy-pr-uk-t-1.glb.ft.com,https://kafka-proxy-pr-uk-t-2.glb.ft.com", "Comma separated kafka proxy hosts for message consuming.")
	consumerGroupID := app.StringOpt("consumer_group_id", "idiConcept", "Kafka group id used for message consuming.")
	consumerOffset := app.StringOpt("consumer_offset", "", "Kafka read offset.")
	consumerAutoCommitEnable := app.BoolOpt("consumer_autocommit_enable", false, "Enable autocommit for small messages.")
	consumerAuthorizationKey := app.StringOpt("consumer_authorization_key", "basic-auth-key", "The authorization key required to UCS access.")

	topic := app.StringOpt("topic", "Concept", "Kafka topic subscribed to")

	consumerConfig := queueConsumer.QueueConfig{}
	consumerConfig.Addrs = strings.Split(*consumerAddrs, ",")
	consumerConfig.Group = *consumerGroupID
	consumerConfig.Topic = *topic
	consumerConfig.Offset = *consumerOffset
	consumerConfig.AuthorizationKey = *consumerAuthorizationKey
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
	baseUrlMap := baseUrlMap{servicesMap}

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
		runServer(baseUrlMap.baseUrlMap, *port, *cacheDuration, *env)
	}

	consumer := queueConsumer.NewConsumer(consumerConfig, baseUrlMap.readMessage, http.Client{})
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

func runServer(baseUrlMap map[string]string, port string, cacheDuration string, env string) {
	var cacheControlHeader string

	if duration, durationErr := time.ParseDuration(cacheDuration); durationErr != nil {
		log.Fatalf("Failed to parse cache duration string, %v", durationErr)
	} else {
		cacheControlHeader = fmt.Sprintf("max-age=%s, public", strconv.FormatFloat(duration.Seconds(), 'f', 0, 64))
	}

	httpHandlers := httpHandlers{cacheControlHeader, baseUrlMap}

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

type baseUrlMap struct {
	baseUrlMap map[string]string
}

func (um baseUrlMap) readMessage(msg queueConsumer.Message) {
	body := strings.NewReader(msg.Body)
	fmt.Printf("Message body is %v", body)
	headers := msg.Headers
	fmt.Printf("Message headers are %v", headers)
	var ingestionType string
	for k, v := range headers {
		if k == "Message-Type" {
			ingestionType = v
		}
	}
	fmt.Printf("Ingestion Type is %v", ingestionType)
	_, err, writerUrl := sendToWriter(ingestionType, body, um.baseUrlMap)

	if err == nil {
		log.Infof("Successfully written msg: %v to writer: %v", msg, writerUrl)
	} else {
		log.Errorf("Error processing msg: %s", msg)
	}
}

func sendToWriter(ingestionType string, msgBody *strings.Reader, urlMap map[string]string) (resp *http.Response, err error, writerUrl string) {
	for messageType, baseUrl := range urlMap {
		if messageType == ingestionType {
			writerUrl = resolveEndpoint(ingestionType, baseUrl)
		}
	}
	fmt.Printf("Writer Url is %v", writerUrl)
	resp, err = http.Post(writerUrl, "application/json", io.Reader(msgBody))
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
	default: //TODO
		return "unknown message type"
	}
}