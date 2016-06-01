Concept Ingester

An API for reading concepts off of the kafka queue and sending them to the appropriate writer.

Installation & running locally

Build
go build github.com/Financial-Times/concept-ingester

Run
$GOPATH/bin/concept-ingester
    --services-list="people-rw-neo4j-blue,organisations-rw-neo4j-blue, etc." \n
    --port="8080" \n
    --consumer_proxy_addr="http://localhost:8080" \n
    --consumer_group_id="Group Id" \n
    --consumer_autocommit_enable=true \n
    --topic="topic" \n
    --consumer_offset="smallest" \n
    --consumer_queue_id="kafka" \n
    --consumer_stream_count=10 \n
