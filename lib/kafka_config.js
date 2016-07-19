"use strict";

const CONSUMER_DEFAULTS = {
    "enable.auto.commit": "false"
};

const CONSUMER_TOPIC_DEFAULTS = {
    'auto.offset.reset': 'largest'
};

const PRODUCER_DEFAULTS = {

};

const PRODUCER_TOPIC_DEFAULTS = {

};

class KafkaConfig {
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
            throw new Error('metadata_broker_list property is required fot the kafka config');
        }

        this._kafkaConf = kafkaConf;

        const consumerTopicConf = Object.assign(CONSUMER_TOPIC_DEFAULTS,
            kafkaConf.consumer && kafkaConf.consumer.default_topic_conf || {});
        const producerTopicConf = Object.assign(PRODUCER_TOPIC_DEFAULTS,
            kafkaConf.producer && kafkaConf.producer.default_topic_conf || {});

        this._consumerConf = Object.assign(CONSUMER_DEFAULTS, kafkaConf.consumer || {});
        this._consumerConf['metadata.broker.list'] = kafkaConf.metadata_broker_list;
        this._consumerConf.default_topic_conf = consumerTopicConf;

        this._producerConf = Object.assign(PRODUCER_DEFAULTS, kafkaConf.producer || {});
        this._producerConf['metadata.broker.list'] = kafkaConf.metadata_broker_list;
        this._producerConf.default_topic_conf = producerTopicConf;
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
     * Returns a consumer configuration object
     */
    consumerConf(groupId, clientId) {
        const conf = Object.assign({}, this._consumerConf);
        conf['group.id'] = groupId;
        conf['client.id'] = '' + (Math.random() * 1000000);
        return conf;
    }

    /**
     * Returns a consumer configuration object
     */
    producerConf() {
        const conf = Object.assign({}, this._producerConf);
        conf['client.id'] = '' + (Math.random() * 1000000);
        return conf;
    }
}
module.exports = KafkaConfig;
