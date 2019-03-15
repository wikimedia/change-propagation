#!/bin/bash

. $(cd $(dirname $0) && pwd)/../../node_modules/kafka-test-tools/clean_kafka.sh

check 2181 "Zookeeper"
check 9092 "Kafka"

# Don't need to clean anything in Jenkins or Travis
if [ "x$JENKINS_URL" = "x"  ] || [ "$CI" = "true" ]; then
  dropTopics "test_dc"
  sleep 5
fi

while read in; do createTopic $in; done < $(dirname $0)/test_topics

wait
sleep 5

redis-cli --raw keys "CP*" | xargs redis-cli del
