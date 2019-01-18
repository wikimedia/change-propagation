'use strict';

const kafka = require('node-rdkafka');
const P = require('bluebird');
const EventEmitter = require('events').EventEmitter;

const CONSUMER_DEFAULTS = {
    // We don't want the driver to commit automatically the offset of just read message,
    // we will handle offsets manually.
    'enable.auto.commit': 'false'
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
     * @inheritdoc
     */
    constructor(conf, topicConf) {
        super(conf, topicConf);

        this.on('delivery-report', (err, report) => {
            const reporter = report.opaque;
            if (err) {
                return reporter.rejecter(err);
            }
            return reporter.resolver(report);
        });

        this.on('ready', () => {
            this._pollInterval = setInterval(() => this.poll(), 10);
        });
    }

    /**
     * @inheritdoc
     */
    produce(topic, partition, message, key) {
        return new P((resolve, reject) => {
            const report = {
                resolver: resolve,
                rejecter: reject
            };
            try {
                const result = super.produce(topic, partition, message, key, undefined, report);
                if (result !== true) {
                    process.nextTick(() => {
                        reject(result);
                    });
                }
            } catch (e) {
                process.nextTick(() => {
                    reject(e);
                });
            } finally {
                this.poll();
            }
        });
    }

    /**
     * @inheritdoc
     */
    disconnect(cb) {
        if (this._pollInterval) {
            clearInterval(this._pollInterval);
        }
        return super.disconnect(cb);
    }

}

class AutopollingProducer extends kafka.Producer {
    constructor(config, topicConfig, logger) {
        super(config, topicConfig);
        this.on('event.error', (e) => logger.log('error/producer', {
            msg: 'Producer error', e
        }));
    }

    /**
     * @inheritdoc
     */
    produce(topic, partition, message, key) {
        return new P((resolve, reject) => {
            try {
                const result = super.produce(topic, partition, message, key);
                if (result !== true) {
                    return reject(result);
                }
                return resolve(result);
            } catch (e) {
                return reject(e);
            } finally {
                this.poll();
            }
        });
    }
}

class MetadataWatch extends EventEmitter {
    constructor(consumer, consumeDC) {
        super();
        this._consumer = consumer;
        this._knownTopics = [];
        this._refreshInterval = undefined;
        this._consumeDC = consumeDC;
    }

    _setup() {
        return this.getTopics()
            .then((topics) => {
                this._knownTopics = topics;
                this._refreshInterval = setInterval(() => {
                    this.getTopics()
                        .then((topics) => {
                            const topicsAdded = topics
                                .some((newTopic) => this._knownTopics.indexOf(newTopic) < 0);
                            if (topicsAdded) {
                                this.emit('topics_changed', topics);
                            }
                            this._knownTopics = topics;
                        })
                        .catch((e) => this.emit('error', e));
                    // TODO: make configurable
                }, 10000);
            })
            .thenReturn(this);
    }

    getTopics() {
        const dcRemoveRegex = new RegExp(`^${this._consumeDC}\\.`);
        return new P((resolve, reject) => {
            this._consumer.getMetadata(undefined, (err, res) => {
                if (err) {
                    return reject(err);
                }
                resolve(res.topics.filter((topicInfo) =>
                    topicInfo.name.startsWith(this._consumeDC))
                    .map((topicInfo) => {
                        if (this._consumeDC) {
                            return topicInfo.name.replace(dcRemoveRegex, '');
                        }
                        return topicInfo.name;
                    }));
            });
        });
    }

    disconnect() {
        if (this._refreshInterval) {
            clearInterval(this._refreshInterval);
        }
        return this._consumer.disconnect();
    }
}

class KafkaFactory {
    /**
     * Contains the kafka consumer/producer configuration. The configuration options
     * are directly passed to librdkafka. For options see librdkafka docs:
     * https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
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
        this._consumerConf[ 'metadata.broker.list' ] = kafkaConf.metadata_broker_list;

        this._producerConf = Object.assign({}, PRODUCER_DEFAULTS, kafkaConf.producer || {});
        this._producerConf[ 'metadata.broker.list' ] = kafkaConf.metadata_broker_list;

        this.startup_delay = kafkaConf.startup_delay || 0;
    }

    /**
     * Returns a DC name to consume from
     * @return {string}
     */
    get consumeDC() {
        return this._kafkaConf.dc_name || this._kafkaConf.consume_dc || 'datacenter1';
    }

    /**
     * Returns a DC name to produce to
     * @return {string}
     */
    get produceDC() {
        return this._kafkaConf.dc_name || this._kafkaConf.produce_dc || 'datacenter1';
    }

    /**
     * Create new KafkaConsumer and connect it.
     * @param {string} groupId Consumer group ID to use
     * @param {Array} topics Topics to subscribe to
     * @param {Object} [metrics] metrics reporter
     * @return {Promise}
     */
    createConsumer(groupId, topics, metrics) {
        const conf = Object.assign({}, this._consumerConf);
        conf[ 'group.id' ] = groupId;
        conf[ 'client.id' ] = `${Math.floor(Math.random() * 1000000)}`;

        return new P((resolve, reject) => {
            const consumer = new kafka.KafkaConsumer(conf, this._consumerTopicConf);
            consumer.connect(undefined, (err) => {
                if (err) {
                    return reject(err);
                }
                consumer.subscribe(topics);
                resolve(P.promisifyAll(consumer));
            });
        });
    }

    createMetadataWatch(groupId) {
        const conf = Object.assign({}, this._consumerConf);
        conf[ 'group.id' ] = groupId;
        conf[ 'client.id' ] = `${Math.floor(Math.random() * 1000000)}`;

        return new P((resolve, reject) => {
            const consumer = new kafka.KafkaConsumer(conf, this._consumerTopicConf);
            consumer.connect(undefined, (err) => {
                if (err) {
                    return reject(err);
                }
                resolve(consumer);
            });
        })
            .then((consumer) => new MetadataWatch(consumer, this.consumeDC)._setup());
    }

    _createProducerOfClass(ProducerClass, logger) {
        return new P((resolve, reject) => {
            const producer = new ProducerClass(
                this._producerConf,
                this._producerTopicConf,
                logger
            );
            producer.once('event.error', reject);
            producer.connect(undefined, (err) => {
                if (err) {
                    return reject(err);
                }
                return resolve(producer);
            });
        });

    }

    createProducer(logger) {
        return this._createProducerOfClass(AutopollingProducer, logger);
    }

    createGuaranteedProducer(logger) {
        return this._createProducerOfClass(GuaranteedProducer, logger);
    }
}
module.exports = KafkaFactory;
