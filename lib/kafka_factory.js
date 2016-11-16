"use strict";

const kafka = require('node-rdkafka');
const P = require('bluebird');

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
    'request.required.acks': 1
};

class GuaranteedProducer extends kafka.Producer {
    /**
     * @inheritDoc
     */
    constructor(conf, topicConf, logger) {
        super(conf, topicConf);
        this._logger = logger;
        this._pending = {};

        this.on('delivery-report', (report) => {
            const reportKey = `${report.topic}:${report.key}`;
            const resolve = this._pending[reportKey];
            if (!resolve) {
                this._logger.log('error/produce', {
                    message: 'Unknown resolver in delivery report',
                    report
                });
                return;
            }

            delete this._pending[reportKey];
            resolve(report);
        });

        this._pollInterval = setInterval(() => this.poll(), 500);
    }

    /**
     * @inheritDoc
     */
    produce(topic, partition, message, key) {
        return new P((resolve, reject) => {
            if (!key) {
                process.nextTick(() =>
                    reject(new Error('Key is required for guaranteed delivery')));
                return;
            }

            const reportKey = `${topic}:${key}`;

            if (this._pending[reportKey]) {
                process.nextTick(() => reject(new Error(`Duplicate key: ${reportKey}`)));
                return;
            }

            this._pending[reportKey] = resolve;

            try {
                const result = super.produce(topic, partition, message, key);
                if (result !== true) {
                    process.nextTick(() => {
                        delete this._pending[reportKey];
                        reject(result);
                    });
                }
            } catch (e) {
                process.nextTick(() => {
                    delete this._pending[reportKey];
                    reject(e);
                });
            }
        });
    }

    /**
     * @inheritDoc
     */
    disconnect(cb) {
        clearInterval(this._pollInterval);
        return super.disconnect(cb);
    }

}

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

    createConsumer(groupId, topic) {
        const conf = Object.assign({}, this._consumerConf);
        conf['group.id'] = groupId;
        conf['client.id'] = `${Math.floor(Math.random() * 1000000)}`;
        return new P((resolve, reject) => {
            const consumer = new kafka.KafkaConsumer(conf, this._consumerTopicConf);
            consumer.connect(undefined, (err) => {
                if (err) {
                    return reject(err);
                }
                consumer.subscribe([ topic ]);
                resolve(P.promisifyAll(consumer));
            });
        });
    }

    _createProducerOfClass(ProducerClass, logger) {
        return new P((resolve, reject) => {
            const producer = new ProducerClass(
                this._producerConf,
                this._producerTopicConf,
                logger || function() {}
            );
            producer.once('error', reject);
            producer.connect(undefined, (err) => {
                if (err) {
                    return reject(err);
                }
                return resolve(producer);
            });
        });

    }

    createProducer(logger) {
        return this._createProducerOfClass(kafka.Producer, logger);
    }

    createGuaranteedProducer(logger) {
        return this._createProducerOfClass(GuaranteedProducer, logger);
    }
}
module.exports = KafkaFactory;
