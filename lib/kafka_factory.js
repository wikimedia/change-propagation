"use strict";

const kafka = require('node-rdkafka');
const P = require('bluebird');
const rdKafkaStatsdCb = require('node-rdkafka-statsd');

/**
 * How often to report statistics. Once per minute is more then enough.
 *
 * @type {number}
 */
const STATS_INTERVAL = 60000;

const CONSUMER_DEFAULTS = {
    // We don't want the driver to commit automatically the offset of just read message,
    // we will handle offsets manually.
    'enable.auto.commit': 'false',
};

const CONSUMER_TOPIC_DEFAULTS = {
    // When we add a new rule we don't want it to reread the whole commit log,
    // but start from the latest message.
    'auto.offset.reset': 'largest'
};

const PRODUCER_DEFAULTS = {
    dr_cb: true
};

const PRODUCER_TOPIC_DEFAULTS = {

};

class KafkaFactory {
    /**
     * Contains the kafka consumer/producer configuration. The configuration options
     * are directly passed to librdkafka. For options see librdkafka docs:
     * https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
     *
     * @param {Object} kafkaConf
     * @param {Object} kafkaConf.metadata_broker_list a list of kafka brokers
     * @param {string} [kafkaConf.consume_dc] a DC name to consume from
     * @param {string} [kafkaConf.produce] a DC name to produce to
     * @param {string} [kafkaConf.dc_name] a DC name to consume from and produce to
     * @param {Object} [kafkaConf.consumer] Consumer configuration.
     * @param {Object} [kafkaConf.producer] Producer configuration.
     */
    constructor(kafkaConf) {
        if (!kafkaConf.metadata_broker_list) {
            throw new Error('metadata_broker_list property is required for the kafka config');
        }

        this._kafkaConf = kafkaConf;

        this._consumerTopicConf = Object.assign({}, CONSUMER_TOPIC_DEFAULTS,
            kafkaConf.consumer && kafkaConf.consumer.default_topic_conf || {});
        if (kafkaConf.consumer) {
            delete kafkaConf.consumer.default_topic_conf;
        }

        this._producerTopicConf = Object.assign({}, PRODUCER_TOPIC_DEFAULTS,
            kafkaConf.producer && kafkaConf.producer.default_topic_conf || {});
        if (kafkaConf.producer) {
            delete kafkaConf.producer.default_topic_conf;
        }

        this._consumerConf = Object.assign({}, CONSUMER_DEFAULTS, kafkaConf.consumer || {});
        this._consumerConf['metadata.broker.list'] = kafkaConf.metadata_broker_list;

        this._producerConf = Object.assign({}, PRODUCER_DEFAULTS, kafkaConf.producer || {});
        this._producerConf['metadata.broker.list'] = kafkaConf.metadata_broker_list;

        this.startup_delay = kafkaConf.startup_delay || 0;
    }

    /**
     * Returns a DC name to consume from
     *
     * @returns {string}
     */
    get consumeDC() {
        return this._kafkaConf.dc_name || this._kafkaConf.consume_dc || 'datacenter1';
    }

    /**
     * Returns a DC name to produce to
     *
     * @returns {string}
     */
    get produceDC() {
        return this._kafkaConf.dc_name || this._kafkaConf.produce_dc || 'datacenter1';
    }

    /**
     * Create new KafkaConsumer and connect it.
     *
     * @param {string} groupId Consumer group ID to use
     * @param {string} topic Topic to subscribe to
     * @param {object} [metrics] metrics reporter
     */
    createConsumer(groupId, topic, metrics) {
        const conf = Object.assign({}, this._consumerConf);
        conf['group.id'] = groupId;
        conf['client.id'] = `${Math.floor(Math.random() * 1000000)}`;

        let statCb;
        if (metrics) {
            statCb = rdKafkaStatsdCb(metrics);
            conf['statistics.interval.ms'] = STATS_INTERVAL;
        }

        return new P((resolve, reject) => {
            const consumer = new kafka.KafkaConsumer(conf, this._consumerTopicConf);
            consumer.connect(undefined, (err) => {
                if (err) {
                    return reject(err);
                }
                consumer.subscribe([ topic ]);
                if (statCb) {
                    consumer.on('event.stats', (stat) => {
                        try {
                            statCb(stat);
                        } catch (e) {
                            // Just to make 100% sure we don't kill
                            // the application on metrics error.
                        }
                    });
                }
                resolve(P.promisifyAll(consumer));
            });
        });
    }

    createProducer() {
        return new P((resolve, reject) => {
            const producer = new kafka.Producer(
                this._producerConf,
                this._producerTopicConf);
            producer.once('error', reject);
            producer.once('ready', () => resolve(producer));
            producer.connect();
        });
    }
}
module.exports = KafkaFactory;
