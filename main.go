package main

import (
	"fmt"
	"io"
	"io/ioutil"
	standardLog "log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Financial-Times/http-handlers-go/httphandlers"
	queueConsumer "github.com/Financial-Times/message-queue-gonsumer/consumer"
	status "github.com/Financial-Times/service-status-go/httphandlers"
	"github.com/asaskevich/govalidator"
	"github.com/cyberdelia/go-metrics-graphite"
	"github.com/gorilla/mux"
	"github.com/jawher/mow.cli"
	"github.com/rcrowley/go-metrics"
	log "github.com/Sirupsen/logrus"
)

func main() {
	log.SetLevel(log.InfoLevel)
	app := cli.App("concept-ingester", "A microservice that consumes concept messages from Kafka and routes them to the appropriate writer")

	services := app.String(cli.StringOpt{
		Name:   "services-list",
		Desc:   "A comma separated list of neo4j writer service addresses",
		EnvVar: "SERVICES",
	})
	elasticServiceAddress := app.String(cli.StringOpt{
		Name:   "elastic-service-address",
		Desc:   "elasticsearch writer service address",
		EnvVar: "ELASTICSEARCH_WRITER_ADDRESS",
	})
	port := app.String(cli.StringOpt{
		Name:   "port",
		Value:  "8080",
		Desc:   "Port to listen on",
		EnvVar: "PORT",
	})
	kafkaProxyAddress := app.String(cli.StringOpt{
		Name:   "kafka-proxy-address",
		Value:  "http://kafka-proxy:8082",
		Desc:   "Kafka proxy address",
		EnvVar: "KAFKA_PROXY_ADDRESS",
	})
	consumerGroupID := app.String(cli.StringOpt{
		Name:   "consumer-group-id",
		Value:  "ConceptIngesterGroup",
		Desc:   "Kafka group id used for message consuming",
		EnvVar: "GROUP_ID",
	})
	consumerQueue := app.String(cli.StringOpt{
		Name:   "consumer-queue-id",
		Value:  "",
		Desc:   "The kafka queue id",
		EnvVar: "QUEUE_ID",
	})
	graphiteTCPAuthority := app.String(cli.StringOpt{
		Name:   "graphite-tcp-address",
		Value:  "",
		Desc:   "Graphite TCP authority, e.g. graphite.ft.com:2003. Leave as default if you do NOT want to output to graphite (e.g. if running locally)",
		EnvVar: "GRAPHITE_TCP_AUTHORITY",
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
		Name:   "consumer-offset",
		Value:  "",
		Desc:   "Kafka read offset",
		EnvVar: "OFFSET"})
	consumerAutoCommitEnable := app.Bool(cli.BoolOpt{
		Name:   "consumer-autocommit-enable",
		Value:  true,
		Desc:   "Enable autocommit for small messages",
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

	app.Action = func() {
		httpClient := &http.Client{
			Transport: &http.Transport{
				Proxy: http.ProxyFromEnvironment,
				DialContext: (&net.Dialer{
					Timeout:   30 * time.Second,
					KeepAlive: 30 * time.Second,
				}).DialContext,
				MaxIdleConnsPerHost:   128,
				TLSHandshakeTimeout:   3 * time.Second,
				ExpectContinueTimeout: 1 * time.Second,
			},
		}

		consumerConfig := queueConsumer.QueueConfig{
			Addrs:                []string{*kafkaProxyAddress},
			Group:                *consumerGroupID,
			Queue:                *consumerQueue,
			Topic:                *topic,
			Offset:               *consumerOffset,
			AutoCommitEnable:     *consumerAutoCommitEnable,
			ConcurrentProcessing: true,
		}

		writerMappings, err := createWriterMappings(*services)
		if err != nil {
			log.Fatal(err)
		}
		elasticsearchWriterBasicMapping, elasticsearchWriterBulkMapping, err := createElasticsearchWriterMappings(*elasticServiceAddress)
		if err != nil {
			log.Fatal(err)
		}

		ing := ingesterService{
			baseURLMappings:      writerMappings,
			elasticWriterAddress: elasticsearchWriterBulkMapping,
			ticker:               time.NewTicker(time.Second / time.Duration(*throttle)),
			client:               httpClient,
		}

		err = outputMetricsIfRequired(*graphiteTCPAuthority, *graphitePrefix, *logMetrics)
		if err != nil {
			log.Fatal(err)
		}

		consumer := queueConsumer.NewConsumer(consumerConfig, ing.readMessage, httpClient)

		var wg sync.WaitGroup
		wg.Add(1)

		go func() {
			consumer.Start()
			wg.Done()
		}()

		go runServer(consumer, ing.baseURLMappings, elasticsearchWriterBasicMapping, *port, httpClient)

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

func createElasticsearchWriterMappings(elasticServiceAddress string) (elasticsearchWriterBasicMapping string, elasticsearchWriterBulkMapping string, err error) {
	if elasticServiceAddress == "" {
		return
	}
	host, err := extractAddressHost(elasticServiceAddress)
	if err != nil {
		return "", "", err
	}
	elasticsearchWriterBasicMapping = elasticServiceAddress
	elasticsearchWriterBulkMapping = elasticServiceAddress + "/bulk"
	log.Infof("Using writer address: %s for service: %s", elasticsearchWriterBasicMapping, host)
	return
}

func createWriterMappings(services string) (map[string]string, error) {
	writerMappings := make(map[string]string)
	servicesSlice := strings.Split(services, ",")
	for _, serviceAddress := range servicesSlice {
		host, err := extractAddressHost(serviceAddress)
		if err != nil {
			return nil, err
		}
		writerMappings[host] = serviceAddress
		log.Infof("Using writer address: %s for service: %s", serviceAddress, host)
	}
	return writerMappings, nil
}

func extractAddressHost(address string) (string, error) {
	if !govalidator.IsURL(address) {
		return "", fmt.Errorf("Address '%s' is not a valid URL", address)
	}
	validURL, err := url.Parse(address)
	if err != nil {
		return "", fmt.Errorf("Failed to parse address %s: %s", address, err)
	}
	authoritySlice := strings.Split(validURL.Host, ":")
	if len(authoritySlice) != 2 {
		return "", fmt.Errorf("Address '%s' is invalid. Example of an expected value 'http://localhost:8080'", address)
	}
	return authoritySlice[0], nil
}

func runServer(consumer queueConsumer.MessageConsumer, baseURLMappings map[string]string, elasticsearchWriterAddress string, port string, client *http.Client) {
	var includeElasticsearchWriter bool
	if elasticsearchWriterAddress != "" {
		includeElasticsearchWriter = true
	}
	eWC := &ElasticsearchWriterConfig{
		includeElasticsearchWriter: includeElasticsearchWriter,
		elasticsearchWriterUrl:     elasticsearchWriterAddress,
	}
	r := router(NewHealthCheck(consumer, getBaseURLs(baseURLMappings), eWC, client))

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

func getBaseURLs(baseURLMappings map[string]string) []string {
	URLs := make([]string, 0, len(baseURLMappings))
	for _, url := range baseURLMappings {
		URLs = append(URLs, url)
	}
	return URLs
}

func router(hc *HealthCheck) http.Handler {
	servicesRouter := mux.NewRouter()

	servicesRouter.HandleFunc("/__health", hc.Health())
	servicesRouter.HandleFunc(status.GTGPath, status.NewGoodToGoHandler(hc.GTG))

	var monitoringRouter http.Handler = servicesRouter
	monitoringRouter = httphandlers.TransactionAwareRequestLoggingHandler(log.StandardLogger(), monitoringRouter)
	monitoringRouter = httphandlers.HTTPMetricsHandler(metrics.DefaultRegistry, monitoringRouter)

	return monitoringRouter
}

type ingesterService struct {
	baseURLMappings      map[string]string
	elasticWriterAddress string
	client               *http.Client
	ticker               *time.Ticker
}

func (ing ingesterService) readMessage(msg queueConsumer.Message) {
	<-ing.ticker.C
	err := ing.processMessage(msg)
	if err != nil {
		log.Errorf("%v", err)
	}
}

func (ing ingesterService) processMessage(msg queueConsumer.Message) error {
	ingestionType, uuid, transactionID := extractMessageTypeAndID(msg.Headers)

	writerAddress, err := resolveWriter(ingestionType, ing.baseURLMappings)
	if err != nil {
		failureMeter := metrics.GetOrRegisterMeter(ingestionType+"-FAILURE", metrics.DefaultRegistry)
		failureMeter.Mark(1)
		log.Infof("Incremented failure count, new count=%d for meter=%s", failureMeter.Count(), ingestionType+"-FAILURE")
		return err
	}

	log.Infof("Processing message to service: %v", writerAddress)
	err = sendToWriter(ingestionType, msg.Body, uuid, transactionID, writerAddress, ing.client)

	if err != nil {
		failureMeter := metrics.GetOrRegisterMeter(ingestionType+"-FAILURE", metrics.DefaultRegistry)
		failureMeter.Mark(1)
		log.Infof("Incremented failure count, new count=%d for meter=%s", failureMeter.Count(), ingestionType+"-FAILURE")
		return err
	}

	if ing.elasticWriterAddress != "" {
		err = sendToWriter(ingestionType, msg.Body, uuid, transactionID, ing.elasticWriterAddress, ing.client)
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

func extractMessageTypeAndID(headers map[string]string) (string, string, string) {
	return headers["Message-Type"], headers["Message-Id"], headers["X-Request-Id"]
}

func sendToWriter(ingestionType string, msgBody string, uuid string, transactionID string, elasticWriter string, client *http.Client) error {
	log.Infof("Sending message to writer: %v", elasticWriter)

	request, reqURL, err := createWriteRequest(ingestionType, strings.NewReader(msgBody), uuid, elasticWriter)
	if err != nil {
		log.Errorf("Cannot create write request: [%v]", err)
	}
	request.ContentLength = -1

	if transactionID != "" {
		request.Header.Set("X-Request-Id", transactionID)
	}

	log.Infof("Sending %s with uuid: %s to %s", ingestionType, uuid, elasticWriter)

	resp, reqErr := client.Do(request)
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
	var writerAddress string
	for service, URL := range URLMappings {
		if strings.Contains(service, ingestionType) {
			writerAddress = URL
		}
	}
	if writerAddress == "" {
		return "", fmt.Errorf("No configured writer for concept: %v", ingestionType)
	}

	return writerAddress, nil
}

func createWriteRequest(ingestionType string, msgBody io.Reader, uuid string, writerAddress string) (*http.Request, string, error) {

	reqURL := writerAddress + "/" + ingestionType + "/" + uuid

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

func outputMetricsIfRequired(graphiteTCPAuthority string, graphitePrefix string, logMetrics bool) error {
	if graphiteTCPAuthority != "" {
		addr, _ := net.ResolveTCPAddr("tcp", graphiteTCPAuthority)
		go graphite.Graphite(metrics.DefaultRegistry, 5*time.Second, graphitePrefix, addr)
	}
	if logMetrics {
		//useful locally
		//messy use of the 'standard' log package here as this method takes the log struct, not an interface, so can't use logrus.Logger
		go metrics.Log(metrics.DefaultRegistry, 60*time.Second, standardLog.New(os.Stdout, "metrics", standardLog.Lmicroseconds))
	}
	return nil
}
