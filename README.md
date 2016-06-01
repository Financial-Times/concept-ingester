Concept Ingester

An API for reading concepts off of the kafka queue and sending them to the appropriate writer.

Installation & running locally

go build github.com/Financial-Times/concept-ingester
    $GOPATH/bin/concept-ingester
        --services-list="people-rw-neo4j-blue,organisations-rw-neo4j-blue, etc." 
        --port="8080" 
        --consumer_proxy_addr="http://localhost:8080" 
        --consumer_group_id="Group Id" 
        --consumer_autocommit_enable=true 
        --topic="topic" 
        --consumer_offset="smallest" 
        --consumer_queue_id="kafka" 
        --consumer_stream_count=10
