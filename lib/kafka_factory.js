"use strict";

const kafka = require('wmf-kafka-node');
const uuid = require('cassandra-uuid');
const P = require('bluebird');

/**
 * Utility class providing high-level interfaces to kafka modules.
 */
class KafkaFactory {
    /**
     * @param {Object} kafkaConf Kafka connection configuration
     * @param {string} kafkaConf.uri Zookeper URI with host and port
     * @param {string} kafkaConf.clientId Client identification string
     * @param {string} [kafkaConf.consume_dc] DC name to consume from
     * @param {string} [kafkaConf.produce_dc] DC name to produce to
     * @param {string} [kafkaConf.dc_name] DC name to use both for
     *                                     production and consumption
     * @constructor
     */
    constructor(kafkaConf) {
        this.kafkaConf = kafkaConf;

        if (!this.kafkaConf.uri) {
            throw new Error('uri config parameter is required by kafka config');
        }
    }

    /**
     * Creates a new kafka client.
     *
     * @returns {Client}
     */
    newClient() {
        const clientId = `${this.kafkaConf.clientId}-${uuid.TimeUuid.now()}-${uuid.Uuid.random()}`;
        return new kafka.Client(this.kafkaConf.uri, clientId, {});
    }

    /**
     * Creates and initializes an new kafka producer.
     *
     * @param {Client} client a kafka client to use.
     * @param {Object} [options] producer options
     *
     * @returns {Promise<HighLevelProducer>}
     */
    newProducer(client, options) {
        return new P((resolve, reject) => {
            const producer = new kafka.HighLevelProducer(client, options || {});
            producer.once('ready', () => resolve(P.promisifyAll(producer)));
            producer.once('error', reject);
        });
    }

    /**
     * Creates a kafka consumer.
     *
     * @param {Client} client a kafka client to use
     * @param {string} topic a topic name to consume
     * @param {string} groupId consumer group ID
     *
     * @returns {Promise} a promise that's resolved when a consumer is ready
     */
    newConsumer(client, topic, groupId) {
        return new P((resolve, reject) => {
            const consumer = new kafka.HighLevelConsumer(client,
                [{
                    topic: `${this.consumeDC}.${topic}`
                }],
                {
                    groupId,
                    autoCommit: false
                });
            consumer.once('error', reject);
            consumer.once('rebalanced', () => resolve(P.promisifyAll(consumer)));
        });
    }

    /**
     * Returns a DC name to consume from
     *
     * @returns {string}
     */
    get consumeDC() {
        return this.kafkaConf.dc_name || this.kafkaConf.consume_dc || 'datacenter1';
    }

    /**
     * Returns a DC name to produce to
     *
     * @returns {string}
     */
    get produceDC() {
        return this.kafkaConf.dc_name || this.kafkaConf.produce_dc || 'datacenter1';
    }
}

module.exports = KafkaFactory;
