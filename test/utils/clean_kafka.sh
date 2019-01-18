#!/bin/bash

. $(cd $(dirname $0) && pwd)/../../node_modules/kafka-test-tools/clean_kafka.sh

check 2181 "Zookeeper"
check 9092 "Kafka"

# Don't need to clean anything in Jenkins or Travis
if [ "x$JENKINS_URL" = "x"  ] || [ "$CI" = "true" ]; then
  dropTopics "test_dc"
  sleep 5
fi

createTopic "test_dc.simple_test_rule"
createTopic "test_dc.simple_test_rule2"
createTopic "test_dc.simple_test_rule3"
createTopic "test_dc.sample_test_rule"
createTopic "test_dc.retry.simple_test_rule"
createTopic "test_dc.changeprop.retry.simple_test_rule2"
createTopic "test_dc.changeprop.retry.simple_test_rule3"
createTopic "test_dc.kafka_producing_rule"
createTopic "test_dc.changeprop.retry.kafka_producing_rule"
createTopic "test_dc.mediawiki.revision-create"
createTopic "test_dc.mediawiki.revision-score"
createTopic "test_dc.changeprop.retry.mediawiki.revision-create"
createTopic "test_dc.changeprop.retry.mediawiki.revision-score"
createTopic "test_dc.mediawiki.page-create"
createTopic "test_dc.changeprop.retry.mediawiki.page-create"
createTopic "test_dc.resource_change"
createTopic "test_dc.changeprop.retry.resource_change"
createTopic "test_dc.changeprop.error"
createTopic "test_dc.mediawiki.page-delete"
createTopic "test_dc.changeprop.retry.mediawiki.page-delete"
createTopic "test_dc.mediawiki.page-move"
createTopic "test_dc.changeprop.retry.mediawiki.page-move"
createTopic "test_dc.mediawiki.page-undelete"
createTopic "test_dc.changeprop.retry.mediawiki.page-undelete"
createTopic "test_dc.mediawiki.revision-visibility-change"
createTopic "test_dc.changeprop.retry.mediawiki.revision-visibility-change"
createTopic "test_dc.change-prop.transcludes.resource-change"
createTopic "test_dc.changeprop.retry.change-prop.transcludes.resource-change"
createTopic "test_dc.change-prop.backlinks.resource-change"
createTopic "test_dc.changeprop.retry.change-prop.backlinks.resource-change"
createTopic "test_dc.mediawiki.page-properties-change"
createTopic "test_dc.changeprop.retry.mediawiki.page-properties-change"
createTopic "test_dc.change-prop.wikidata.resource-change"
createTopic "test_dc.changeprop.retry.change-prop.wikidata.resource-change"
createTopic "test_dc.mediawiki.job.updateBetaFeaturesUserCounts"
createTopic "test_dc.cpjobqueue.retry.mediawiki.job.updateBetaFeaturesUserCounts"
createTopic "test_dc.mediawiki.job.htmlCacheUpdate"
createTopic "test_dc.cpjobqueue.retry.mediawiki.job.htmlCacheUpdate"
createTopic "test_dc.mediawiki.job.refreshLinks"
createTopic "test_dc.cpjobqueue.retry.mediawiki.job.refreshLinks"
createTopic "test_dc.change-prop.partitioned.mediawiki.job.refreshLinks"  8
createTopic "test_dc.cpjobqueue.retry.change-prop.partitioned.mediawiki.job.refreshLinks" 8

wait
sleep 5

redis-cli --raw keys "CP*" | xargs -r redis-cli del
