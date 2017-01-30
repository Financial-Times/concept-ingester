package main

import (
	"fmt"
	"github.com/Financial-Times/go-fthealth/v1a"
	"github.com/Financial-Times/http-handlers-go/httphandlers"
	queueConsumer "github.com/Financial-Times/message-queue-gonsumer/consumer"
	status "github.com/Financial-Times/service-status-go/httphandlers"
	"github.com/Shopify/sarama"
	log "github.com/Sirupsen/logrus"
	graphite "github.com/cyberdelia/go-metrics-graphite"
	"github.com/gorilla/mux"
	"github.com/jawher/mow.cli"
	"github.com/rcrowley/go-metrics"
	"io"
	"io/ioutil"
	standardLog "log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

var httpClient = http.Client{
	Transport: &http.Transport{
		MaxIdleConnsPerHost: 128,
		Dial: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).Dial,
	},
}

func main() {
	log.SetLevel(log.InfoLevel)
	app := cli.App("concept-ingester", "A microservice that consumes concept messages from Kafka and routes them to the appropriate writer")

	services := app.String(cli.StringOpt{
		Name:   "services-list",
		Value:  "services",
		Desc:   "neo4j writer services",
		EnvVar: "SERVICES",
	})
	elasticService := app.String(cli.StringOpt{
		Name:   "elastic-service",
		Desc:   "elasticsearch writer service",
		EnvVar: "ELASTICSEARCH_WRITER",
	})
	port := app.String(cli.StringOpt{
		Name:   "port",
		Value:  "8080",
		Desc:   "Port to listen on",
		EnvVar: "PORT",
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
		Desc:   "The kafka queue id",
		EnvVar: "QUEUE_ID",
	})
	graphiteTCPAddress := app.String(cli.StringOpt{
		Name:   "graphite-tcp-address",
		Value:  "",
		Desc:   "Graphite TCP address, e.g. graphite.ft.com:2003. Leave as default if you do NOT want to output to graphite (e.g. if running locally)",
		EnvVar: "GRAPHITE_TCP_ADDRESS",
	})
	graphitePrefix := app.String(cli.StringOpt{
		Name:   "graphite-prefix",
		Value:  "",
		Desc:   "Prefix to use. Should start with content, include the environment, and the host name. e.g. coco.pre-prod.special-reports-rw-neo4j.1",
		EnvVar: "GRAPHITE_PREFIX",
	})
	logMetrics := app.Bool(cli.BoolOpt{
		Name:   "log-metrics",
		Value:  false,
		Desc:   "Whether to log metrics. Set to true if running locally and you want metrics output",
		EnvVar: "LOG_METRICS",
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
	topic := app.String(cli.StringOpt{
		Name:   "topic",
		Value:  "kafka-topic",
		Desc:   "Kafka topic subscribed to",
		EnvVar: "TOPIC"})
	throttle := app.Int(cli.IntOpt{
		Name:   "throttle",
		Value:  1000,
		Desc:   "Throttle",
		EnvVar: "THROTTLE"})
	kafkaAddrs := app.String(cli.StringOpt{
		Name:   "kafka_addrs",
		Value:  "localhost:9092",
		Desc:   "Kafka broker addresses",
		EnvVar: "KAFKA_ADDRS"})

	app.Action = func() {
		consumerConfig := queueConsumer.QueueConfig{
			Addrs:                strings.Split(*vulcanAddr, ","),
			Group:                *consumerGroupID,
			Queue:                *consumerQueue,
			Topic:                *topic,
			Offset:               *consumerOffset,
			AutoCommitEnable:     *consumerAutoCommitEnable,
			ConcurrentProcessing: true,
		}

		saramaConsumer, err := sarama.NewConsumer([]string{*kafkaAddrs}, nil)
		if err != nil {
			panic(err)
		}
		defer func() {
			if err := saramaConsumer.Close(); err != nil {
				log.Fatalln(err)
			}
		}()
		partitionConsumer, err := saramaConsumer.ConsumePartition("TestBridge", 0, sarama.OffsetNewest)
		if err != nil {
			panic(err)
		}

		defer func() {
			if err := partitionConsumer.Close(); err != nil {
				log.Fatalln(err)
			}
		}()

		// Trap SIGINT to trigger a shutdown.
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, os.Interrupt)

		writerMappings := createWriterMappings(*services, *vulcanAddr)

		var elasticsearchWriterBasicMapping string
		var elasticsearchWriterBulkMapping string
		if *elasticService != "" {
			elasticsearchWriterBasicMapping = resolveWriterURL(*elasticService, *vulcanAddr)
			if elasticsearchWriterBasicMapping != "" {
				elasticsearchWriterBulkMapping = elasticsearchWriterBasicMapping + "/bulk"
			}
			log.Infof("Using writer url: %s for service: %s", elasticsearchWriterBasicMapping, *elasticService)
		}

		ing := ingesterService{
			baseURLMappings:  writerMappings,
			elasticWriterURL: elasticsearchWriterBulkMapping,
			ticker:           time.NewTicker(time.Second / time.Duration(*throttle)),
		}

		go func() {
			log.Infof("Starting to consume kafka=%v and topic=%v", *kafkaAddrs, "TestBridge")
		ConsumerLoop:
			for {
				select {
				case msg := <-partitionConsumer.Messages():
					log.Info("Consumed message offset %d\n", msg.Offset)
					ftMsg, err := parseMessage(msg.Value)
					if err != nil {
						log.Errorf("Error parsing message : %v", err.Error())
					}
					if err = ing.processMessage(ftMsg); err != nil {
						log.Errorf("Error processing message : %v", err.Error())
					}

				case <-signals:
					break ConsumerLoop
				}
			}
			log.Infof("Stopped consuming from kafka=%v and topic=%v", *kafkaAddrs, "TestBridge")
		}()
		outputMetricsIfRequired(*graphiteTCPAddress, *graphitePrefix, *logMetrics)

		consumer := queueConsumer.NewConsumer(consumerConfig, ing.readMessage, httpClient)

		var wg sync.WaitGroup
		wg.Add(1)

		go func() {
			consumer.Start()
			wg.Done()
		}()

		go runServer(ing.baseURLMappings, elasticsearchWriterBasicMapping, *port, *vulcanAddr, *topic)

		ch := make(chan os.Signal)
		signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)

		<-ch
		log.Println("Shutting down application...")

		consumer.Stop()
		wg.Wait()

		log.Println("Application closing")
	}
	app.Run(os.Args)
}

func createWriterMappings(services string, vulcanAddr string) map[string]string {
	writerMappings := make(map[string]string)
	serviceSlice := strings.Split(services, ",")
	for _, service := range serviceSlice {
		writerURL := resolveWriterURL(service, vulcanAddr)
		writerMappings[service] = writerURL
		log.Infof("Using writer url: %s for service: %s", writerURL, service)
	}
	return writerMappings
}
func resolveWriterURL(service string, vulcanAddr string) string {
	wr := strings.Split(service, ":")
	if len(wr) > 1 {
		return "http://localhost:" + wr[1]
	}
	return vulcanAddr + "/__" + service
}

func runServer(baseURLMappings map[string]string, elasticsearchWriter string, port string, vulcanAddr string, topic string) {

	httpHandlers := httpHandlers{baseURLMappings, elasticsearchWriter, vulcanAddr, topic}
	var r http.Handler
	if elasticsearchWriter != "" {
		r = router(httpHandlers, true)
	} else {
		r = router(httpHandlers, false)
	}

	// The following endpoints should not be monitored or logged (varnish calls one of these every second, depending on config)
	// The top one of these build info endpoints feels more correct, but the lower one matches what we have in Dropwizard,
	// so it's what apps expect currently same as ping, the content of build-info needs more definition
	http.HandleFunc(status.PingPath, status.PingHandler)
	http.HandleFunc(status.PingPathDW, status.PingHandler)
	http.HandleFunc(status.BuildInfoPath, status.BuildInfoHandler)
	http.HandleFunc(status.BuildInfoPathDW, status.BuildInfoHandler)
	log.Infof("concept-ingester-go-app will listen on port: %s", port)

	http.Handle("/", r)

	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("Unable to start server: %v\n", err)
	}
}

func router(hh httpHandlers, includeElasticsearchWriter bool) http.Handler {

	servicesRouter := mux.NewRouter()
	var checks []v1a.Check = []v1a.Check{hh.kafkaProxyHealthCheck(), hh.kafkaProxyHealthCheck()}

	if includeElasticsearchWriter {
		checks = append(checks, hh.elasticHealthCheck())
	}

	servicesRouter.HandleFunc("/__health", v1a.Handler("ConceptIngester Healthchecks", "Checks for accessing writer", checks...))
	servicesRouter.HandleFunc("/__gtg", hh.goodToGo)

	var monitoringRouter http.Handler = servicesRouter
	monitoringRouter = httphandlers.TransactionAwareRequestLoggingHandler(log.StandardLogger(), monitoringRouter)
	monitoringRouter = httphandlers.HTTPMetricsHandler(metrics.DefaultRegistry, monitoringRouter)

	return monitoringRouter
}

type ingesterService struct {
	baseURLMappings  map[string]string
	elasticWriterURL string
	client           http.Client
	ticker           *time.Ticker
}

func (ing ingesterService) readMessage(msg queueConsumer.Message) {
	<-ing.ticker.C
	err := ing.processMessage(msg)
	if err != nil {
		log.Errorf("%v", err)
	}
}

func (ing ingesterService) processMessage(msg queueConsumer.Message) error {
	ingestionType, uuid := extractMessageTypeAndId(msg.Headers)

	writerUrl, err := resolveWriter(ingestionType, ing.baseURLMappings)
	if err != nil {
		failureMeter := metrics.GetOrRegisterMeter(ingestionType+"-FAILURE", metrics.DefaultRegistry)
		failureMeter.Mark(1)
		log.Infof("Incremented failure count, new count=%d for meter=%s", failureMeter.Count(), ingestionType+"-FAILURE")
		return err
	}

	err = sendToWriter(ingestionType, msg.Body, uuid, writerUrl)
	if err != nil {
		failureMeter := metrics.GetOrRegisterMeter(ingestionType+"-FAILURE", metrics.DefaultRegistry)
		failureMeter.Mark(1)
		log.Infof("Incremented failure count, new count=%d for meter=%s", failureMeter.Count(), ingestionType+"-FAILURE")
		return err
	}

	if ing.elasticWriterURL != "" {
		err = sendToWriter(ingestionType, msg.Body, uuid, ing.elasticWriterURL)
		if err != nil {
			failureMeter := metrics.GetOrRegisterMeter(ingestionType+"-elasticsearch-FAILURE", metrics.DefaultRegistry)
			failureMeter.Mark(1)
			log.Infof("Incremented failure count, new count=%d for meter=%s", failureMeter.Count(), ingestionType+"-elasticsearch-FAILURE")
			return err
		}
	}

	successMeter := metrics.GetOrRegisterMeter(ingestionType+"-SUCCESS", metrics.DefaultRegistry)
	successMeter.Mark(1)
	return nil

}

func extractMessageTypeAndId(headers map[string]string) (string, string) {
	return headers["Message-Type"], headers["Message-Id"]
}

func sendToWriter(ingestionType string, msgBody string, uuid string, elasticWriter string) error {
	log.Infof("sendToWriter: concept=[%s] uuid=[%s]", ingestionType, uuid)
	request, reqURL, err := createWriteRequest(ingestionType, strings.NewReader(msgBody), uuid, elasticWriter)
	if err != nil {
		log.Errorf("Cannot read error body: [%v]", err)
		return fmt.Errorf("reqURL=[%s] concept=[%s] uuid=[%s] error=[%v]", reqURL, ingestionType, uuid, err)
	}
	request.ContentLength = -1
	resp, reqErr := httpClient.Do(request)
	if reqErr != nil {
		return fmt.Errorf("reqURL=[%s] concept=[%s] uuid=[%s] error=[%v]", reqURL, ingestionType, uuid, reqErr)
	}
	if resp.StatusCode == http.StatusOK {
		readBody(resp)
		return nil
	}
	defer resp.Body.Close()
	errorMessage, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Errorf("Cannot read error body: [%v]", err)
	}
	return fmt.Errorf("reqURL=[%s] status=[%d] uuid=[%s] error=[%v] body=[%s]", reqURL, resp.StatusCode, uuid, reqErr, string(errorMessage))
}

func resolveWriter(ingestionType string, URLMappings map[string]string) (string, error) {
	var writerURL string
	for service, URL := range URLMappings {
		if strings.Contains(service, ingestionType) {
			writerURL = URL
		}
	}
	if writerURL == "" {
		return "", fmt.Errorf("No configured writer for concept: %v", ingestionType)
	}

	return writerURL, nil
}

func createWriteRequest(ingestionType string, msgBody io.Reader, uuid string, writerURL string) (*http.Request, string, error) {

	reqURL := writerURL + "/" + ingestionType + "/" + uuid

	request, err := http.NewRequest("PUT", reqURL, msgBody)
	if err != nil {
		return nil, reqURL, fmt.Errorf("Failed to create request to %v with body %v", reqURL, msgBody)
	}
	return request, reqURL, err
}

func readBody(resp *http.Response) {
	io.Copy(ioutil.Discard, resp.Body)
	resp.Body.Close()
}

func outputMetricsIfRequired(graphiteTCPAddress string, graphitePrefix string, logMetrics bool) {
	if graphiteTCPAddress != "" {
		addr, _ := net.ResolveTCPAddr("tcp", graphiteTCPAddress)
		go graphite.Graphite(metrics.DefaultRegistry, 5*time.Second, graphitePrefix, addr)
	}
	if logMetrics { //useful locally
		//messy use of the 'standard' log package here as this method takes the log struct, not an interface, so can't use logrus.Logger
		go metrics.Log(metrics.DefaultRegistry, 60*time.Second, standardLog.New(os.Stdout, "metrics", standardLog.Lmicroseconds))
	}
}
