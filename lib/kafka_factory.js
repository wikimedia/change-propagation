"use strict";

var kafka = require('wmf-kafka-node');
var uuid = require('cassandra-uuid');
var P = require('bluebird');

/**
 * Utility class providing high-level interfaces to kafka modules
 * @param {Object} kafkaConf Kafka connection configuration
 * @param {string} kafkaConf.uri Zookeper URI
 * @param {string} kafkaConf.clientId Client identification string
 * @constructor
 */
function KafkaFactory(kafkaConf) {
    this.kafkaConf = kafkaConf;
}

KafkaFactory.prototype.newClient = function() {
    return new kafka.Client(this.kafkaConf.uri,
            this.kafkaConf.clientId + '-' + uuid.TimeUuid.now() + '-' + uuid.Uuid.random(),
            {}
    );
};

KafkaFactory.prototype.newProducer = function(client, options) {
    return new P(function(resolve, reject) {
        var producer = new kafka.HighLevelProducer(client, options);
        producer.once('ready', function() {
            resolve(producer);
        });
        producer.once('error', reject);
    });
};

KafkaFactory.prototype.createTopics = function(producer, topics) {
    return new P(function(resolve, reject) {
        producer.createTopics(topics, false, function(err, data) {
            if (err) {
                reject(err);
            } else {
                resolve(data);
            }
        });
    });
};

KafkaFactory.prototype.getOffsetForTimestamp = function(client, topic, timestamp) {
    return new P(function(resolve, reject) {
        var offset = new kafka.Offset(client);
        offset.once('connect', function() {
            offset.fetch([{
                topic: topic,
                time: timestamp,
                maxNum: 1
            }], function(err, data) {
                if (err) {
                    reject(err);
                } else {
                    var offsetList = data[topic]['0'];
                    resolve(offsetList.length ? offsetList[0] : 0);
                }
            });
        });
    })
};

KafkaFactory.prototype.newConsumer = function(client, topic, groupId, offset) {
    var topicConf = { topic: topic };
    if (offset !== undefined) {
        topicConf.offset = offset;
    }
    return new P(function(resolve, reject) {
        var consumer = new kafka.HighLevelConsumer(client, [ topicConf ], { groupId: groupId });
        consumer.once('error', reject);
        consumer.once('rebalanced', function() {
            resolve(consumer);
        });
    });
};

module.exports = KafkaFactory;
