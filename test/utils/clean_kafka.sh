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
sleep 1
