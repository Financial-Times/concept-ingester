# Concept Ingester

__An API for reading concepts off of the kafka queue and sending them to the appropriate writer__

## Installation & running locally

* `go get github.com/Financial-Times/concept-ingester`
* `cd $GOPATH/src/github.com/Financial-Times/concept-ingester`
* `go install`
* `$GOPATH/bin/concept-ingester --services-list="people-rw-neo4j-blue,organisations-rw-neo4j-blue" --port="8081" --vulcan_addr="http://localhost:8080" --consumer_group_id="TestConcepts" --consumer_autocommit_enable=true --topic="Concept" --consumer_offset="smallest" --consumer_queue_id="kafka" --throttle=10`

## Healthchecks
* Check connectivity [http://localhost:8080/__health](http://localhost:8080/__health)
* Good to Go: [http://localhost:8080/__gtg](http://localhost:8080/__gtg)

##Examples:
### How to run locally not in the cluster
`concept-ingester --services-list="alphaville-series-rw-neo4j:8092" --port="8089" --vulcan_addr="http://localhost:8082" --consumer_group_id="alphaville-series" --topic="Concept" --consumer_of fset="smallest" --consumer_queue_id="kafka" 
`  
This run configuration overrides host:port for kafka-rest-proxy (--vulcan_addr) and supplies the port for the xxx-rw-neo4j service that is assumed to be running on localhost.
If the app is to run in the cluster there is no need to append port to the writer service.  

Some comments about configuration parameters:  
* --vulcan_addr     takes a list of hosts and if not in the cluster is an address of the kafka-rest-proxy (in the cluster it is resolved to vulkan address)  
* --services-list   comma separated list of neo4j writers - one per concept  
* --port                port of this app   
