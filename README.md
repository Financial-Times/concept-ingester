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
