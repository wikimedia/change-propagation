#!/bin/bash

source "$(dirname "$0")"/../../node_modules/kafka-test-tools/clean_kafka.sh

check 2181 "Zookeeper"
check 9092 "Kafka"

# Don't need to clean anything in Jenkins
if [ "x$JENKINS_URL" = "x" ]; then
  dropTopics "test_dc"
  sleep 5
fi

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
createTopic "test_dc.mediawiki.revision-visibility-change"
createTopic "test_dc.change-prop.retry.mediawiki.revision-visibility-change"
createTopic "test_dc.change-prop.transcludes.resource-change"
createTopic "test_dc.change-prop.retry.change-prop.transcludes.resource-change"
createTopic "test_dc.change-prop.backlinks.resource-change"
createTopic "test_dc.change-prop.retry.change-prop.backlinks.resource-change"
createTopic "test_dc.mediawiki.page-properties-change"
createTopic "test_dc.change-prop.retry.mediawiki.page-properties-change"
createTopic "test_dc.change-prop.wikidata.resource-change"
createTopic "test_dc.change-prop.retry.test_dc.change-prop.wikidata.resource-change"

wait
sleep 5
