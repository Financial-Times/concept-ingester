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
* `$GOPATH/bin/concept-ingester --service-authorities-list="http://people-rw-neo4j-blue:8080,organisations-rw-neo4j-blue:8080" --port="8081" --kafka-proxy-address="http://localhost:8080" --consumer-group-id="TestConcepts" --consumer-autocommit-enable=true --topic="Concept" --consumer-offset="smallest" --consumer-queue-id="kafka" --throttle=10`

Some comments about configuration parameters:  
* --kafka-proxy-address     the kafka proxy address, host and port
* --service-authorities     comma separated list of neo4j writers authorities, each pair should be provided as host:port
* --elastic-service-address elasticsearch writer address, host and port
* --topic, --consumer-group-id, --consumer-autocommit-enable, --consumer-offset, --consumer-queue-id see the message-queue-gonsumer library

## Healthchecks
* Check connectivity [http://localhost:8080/__health](http://localhost:8080/__health)
* Good to Go: [http://localhost:8080/__gtg](http://localhost:8080/__gtg)

##Examples:
### How to run locally not in the cluster
`concept-ingester --service-authorities-list="http://localhost:8092" --port="8089" --kafka-proxy-address="http://localhost:8082" --consumer-group-id="alphaville-series" --topic="Concept" --consumer-offset="smallest" --consumer-queue-id="kafka"
`  
To run locally against local writers just use localhost and the port each writer is running on. Override the kafka-address parameter to point to the host:port of a kafka-rest-proxy.
