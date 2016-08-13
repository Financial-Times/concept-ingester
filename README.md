# Concept Ingester

__An API for reading concepts off of the kafka queue and sending them to the appropriate writer__

## Installation

* `go get github.com/Financial-Times/concept-ingester`
* `cd $GOPATH/src/github.com/Financial-Times/concept-ingester`
* `go install`

## Running in a cluster
* `$GOPATH/bin/concept-ingester --services-list="people-rw-neo4j-blue,organisations-rw-neo4j-blue" --port="8081" --vulcan_addr="http://localhost:8080" --consumer_group_id="TestConcepts" --consumer_autocommit_enable=true --topic="Concept" --consumer_offset="smallest" --consumer_queue_id="kafka" --throttle=10`

Some comments about configuration parameters:  
* --vulcan_addr     the vulcan address, host and port
* --services-list   comma separated list of neo4j writers - do not append a port for running in the cluster
* --topic, --consumer_group_id, --consumer_autocommit_enable, --consumer_offset, --consumer_queue_id see the message-queue-gonsumer library  

## Healthchecks
* Check connectivity [http://localhost:8080/__health](http://localhost:8080/__health)
* Good to Go: [http://localhost:8080/__gtg](http://localhost:8080/__gtg)

##Examples:
### How to run locally not in the cluster
`concept-ingester --services-list="alphaville-series-rw-neo4j:8092" --port="8089" --vulcan_addr="http://localhost:8082" --consumer_group_id="alphaville-series" --topic="Concept" --consumer_of fset="smallest" --consumer_queue_id="kafka"
`  
To run locally against local writers, specify the port each of these writers are running on (the writer address will be resolved to localhost:[specified-port]). Override the vulcan-addr parameter to point to the host:port of a kafka-rest-proxy.
