#!/bin/bash

if [ "x$KAFKA_HOME" = "x" ]; then
  echo "Please set KAFKA_HOME env variable to the kafka install directory"
  exit 1
fi

if [ "$1" = "start" ]; then
  if [ `nc localhost 2181 < /dev/null; echo $?` != 0 ]; then
    sh $KAFKA_HOME/bin/zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties > /dev/null &
    while [ `nc localhost 2181 < /dev/null; echo $?` != 0 ]; do
      echo "waiting for Zookeeper..."
      sleep 1 ;
    done
  else
    echo "Zookeper already running"
  fi

  if [ `nc localhost 9092 < /dev/null; echo $?` != 0 ]; then
    sh $KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties > /dev/null &
    while [ `nc localhost 9092 < /dev/null; echo $?` != 0 ]; do
      echo "waiting for Kafka..." ;
      sleep 1 ;
    done
  else
    echo "Kafka already running";
  fi
elif [ "$1" = "stop" ]; then
  sh $KAFKA_HOME/bin/kafka-server-stop.sh &
  sh $KAFKA_HOME/bin/zookeeper-server-stop.sh &
fi