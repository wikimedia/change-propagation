#!/bin/bash

dropTopics ( ) {
  if [ "$#" -eq 1 ]
  then
    PATTERN=$1
    echo "looking for topics named '*${PATTERN}*'..."
    TOPICS=`${KAFKA_HOME}/bin/kafka-topics.sh --zookeeper localhost:2181 --list \
    	| grep ${PATTERN} \
    	| grep -v 'marked for deletion$'`
    for TOPIC in ${TOPICS}
    do
      echo "dropping topic ${TOPIC}"
      ${KAFKA_HOME}/bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic ${TOPIC} > /dev/null
    done
  fi
}

createTopic ( ) {
    echo "Creating topic ${1}"
    ${KAFKA_HOME}/bin/kafka-topics.sh --create \
        --zookeeper 127.0.0.1:2181             \
        --partitions 1                         \
        --replication-factor 1                 \
        --topic $1 > /dev/null
}

check ( ) {
  PORT=$1
  SERVICE_NAME=$2
  if [ `nc localhost ${PORT} < /dev/null; echo $?` != 0 ]; then
    echo "${SERVICE_NAME} not running, start it first with npm run start-kafka"
    exit 1
  fi
}

check 2181 "Zookeeper"
check 9092 "Kafka"
dropTopics "test_dc"
sleep 5

createTopic "test_dc.simple_test_rule"
createTopic "test_dc.change-prop.retry.simple_test_rule"
createTopic "test_dc.kafka_producing_rule"
createTopic "test_dc.change-prop.retry.kafka_producing_rule"
createTopic "test_dc.mediawiki.revision-create"
createTopic "test_dc.change-prop.retry.mediawiki.revision-create"
createTopic "test_dc.change-prop.backlinks.continue"
createTopic "test_dc.change-prop.retry.change-prop.backlinks.continue"
createTopic "test_dc.change-prop.transcludes.continue"
createTopic "test_dc.change-prop.retry.change-prop.transcludes.continue"
createTopic "test_dc.resource_change"
createTopic "test_dc.change-prop.retry.resource_change"
createTopic "test_dc.change-prop.error"
createTopic "test_dc.mediawiki.page-delete"
createTopic "test_dc.change-prop.retry.mediawiki.page-delete"
createTopic "test_dc.mediawiki.page-move"
createTopic "test_dc.change-prop.retry.mediawiki.page-move"
createTopic "test_dc.mediawiki.page-undelete"
createTopic "test_dc.change-prop.retry.mediawiki.page-undelete"
createTopic "test_dc.mediawiki.revision-visibility-set"
createTopic "test_dc.change-prop.retry.mediawiki.revision-visibility-set"
createTopic "test_dc.change-prop.transcludes.resource-change"
createTopic "test_dc.change-prop.retry.change-prop.transcludes.resource-change"
sleep 5
