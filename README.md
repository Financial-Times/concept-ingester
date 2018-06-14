*DECOMISSIONED*

# Concept Ingester

[![Circle CI](https://circleci.com/gh/Financial-Times/concept-ingester.svg?style=shield)](https://circleci.com/gh/Financial-Times/concept-ingester)[![Go Report Card](https://goreportcard.com/badge/github.com/Financial-Times/concept-ingester)](https://goreportcard.com/report/github.com/Financial-Times/concept-ingester) [![Coverage Status](https://coveralls.io/repos/github/Financial-Times/concept-ingester/badge.svg)](https://coveralls.io/github/Financial-Times/concept-ingester)

__An API for reading concepts off of the kafka queue and sending them to the appropriate neo4j writer__

An elasticsearch writer can also be configured. If so, after a successful ingestion into neo4j, the ingester will ingest the concept into elasticsearch too.
Incremental counters both for neo and elasticsearch are configured.

## Installation

* `go get github.com/Financial-Times/concept-ingester`
* `cd $GOPATH/src/github.com/Financial-Times/concept-ingester`
* `go install`

## Running in a cluster
* `$GOPATH/bin/concept-ingester --services-list="people-rw-neo4j-blue,organisations-rw-neo4j-blue" --port="8081" --vulcan_addr="http://localhost:8080" --consumer_group_id="TestConcepts" --consumer_autocommit_enable=true --topic="Concept" --consumer_offset="smallest" --consumer_queue_id="kafka" --throttle=10`

Some comments about configuration parameters:  
* --vulcan_addr     the vulcan address, host and port
* --services-list   comma separated list of neo4j writers - do not append a port for running in the cluster
* --elastic-service elasticsearch writer name
* --topic, --consumer_group_id, --consumer_autocommit_enable, --consumer_offset, --consumer_queue_id see the message-queue-gonsumer library  
* the `--consumer_queue_id/QUEUE_ID` is used as a switch between clusters with vulcan-routing and those without - if this param is set, we assume vulcan-based routing.
## NOTE

This concept publishing pipeline is nearing end of life. It can currently only used to publish all organisations, factset people and authors. Most TME concepts have been switched over to use the new concept publishing pipeline described in detail [here](https://sites.google.com/a/ft.com/universal-publishing/documentation/introduction-to-metadata) which are published via the [basic-tme-transformer](https://github.com/Financial-Times/basic-tme-transformer)

## Healthchecks
* Check connectivity [http://localhost:8080/__health](http://localhost:8080/__health)
* Good to Go: [http://localhost:8080/__gtg](http://localhost:8080/__gtg)

##Examples:
### How to run locally not in the cluster
`concept-ingester --services-list="alphaville-series-rw-neo4j:8092" --port="8089" --vulcan_addr="http://localhost:8082" --consumer_group_id="alphaville-series" --topic="Concept" --consumer_of fset="smallest" --consumer_queue_id="kafka"
`  
To run locally against local writers, specify the port each of these writers are running on (the writer address will be resolved to localhost:[specified-port]). Override the vulcan-addr parameter to point to the host:port of a kafka-rest-proxy.
