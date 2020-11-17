'use strict';

/**
 * restbase-mod-queue-kafka main entry point
 */

const P = require('bluebird');
const HyperSwitch = require('hyperswitch');
const HTTPError = HyperSwitch.HTTPError;
const uuidv1 = require('uuid').v1;

const utils = require('../lib/utils');
const kafkaFactory = require('../lib/kafka_factory');
const RuleSubscriber = require('../lib/rule_subscriber');

class Kafka {
    constructor(options) {
        this.options = options;
        this.kafkaFactory = kafkaFactory.getFactory(options);
        this.staticRules = options.templates || {};

        this.subscriber = new RuleSubscriber(options, this.kafkaFactory);
    }

    setup(hyper) {
        return this.kafkaFactory.createGuaranteedProducer(hyper.logger)
        .then((producer) => {
            this.producer = producer;
            this._connected = true;
            HyperSwitch.lifecycle.once('close', () => {
                this._connected = false;
                this.subscriber.unsubscribeAll();
                this.producer.disconnect();
            });
            return this._subscribeRules(hyper, this.staticRules);
        })
        .tap(() => hyper.logger.log('info/change-prop/init', 'Kafka Queue module initialised'));
    }

    _subscribeRules(hyper, rules) {
        return P.map(Object.keys(rules), ruleName =>
            this.subscriber.subscribe(hyper, ruleName, rules[ruleName]))
        .thenReturn({ status: 201 });
    }

    subscribe(hyper, req) {
        return this._subscribeRules(hyper, req.body);
    }

    produce(hyper, req) {
        if (this.options.test_mode) {
            hyper.logger.log('trace/produce', 'Running in TEST MODE; Production disabled');
            return { status: 201 };
        }

        if (!this._connected) {
            hyper.logger.log('debug/produce', 'Attempt to produce while disconnected');
            return { status: 202 };
        }

        const partition = req.params.partition || null;
        const messages = req.body;
        if (!Array.isArray(messages) || !messages.length) {
            throw new HTTPError({
                status: 400,
                body: {
                    type: 'bad_request',
                    detail: 'Events should be a non-empty array'
                }
            });
        }
        // Check whether all messages contain the topic
        messages.forEach((message) => {
            const now = new Date();
            message.meta.id = message.meta.id || uuidv1({ msecs: now.getTime() });
            message.meta.dt = message.meta.dt || now.toISOString();
            message.meta.request_id = message.meta.request_id || utils.requestId();
        });
        return P.all(messages.map((message) => {
            if (!message.meta.stream) {
                throw new HTTPError({
                    status: 400,
                    body: {
                        type: 'bad_request',
                        detail: 'Event must have a meta.stream',
                        event_str: message
                    }
                });
            }
            hyper.metrics.makeMetric({
                type: 'Counter',
                name: 'produce',
                prometheus: {
                    name: 'changeprop_kafka_produce_count_total',
                    help: 'kafka produce count'
                },
                labels: {
                    names: ['request_class', 'type', 'partition'],
                    omitLabelNames: true
                }
            }).increment(1, [hyper.requestClass, message.meta.stream.replace(/\./g, '_'), partition]);
            return this.producer.produce(`${this.kafkaFactory.produceDC}.${message.meta.stream}`,
                partition,
                Buffer.from(JSON.stringify(message)));
        }))
        .thenReturn({ status: 201 });
    }
}

module.exports = (options) => {
    const kafkaMod = new Kafka(options);
    return {
        spec: {
            paths: {
                '/setup': {
                    put: {
                        summary: 'set up the kafka listener',
                        operationId: 'setup_kafka'
                    }
                },
                '/events{/partition}': {
                    post: {
                        summary: 'produces a message the kafka topic',
                        operationId: 'produce'
                    }
                },
                '/subscriptions': {
                    post: {
                        summary: 'adds a new subscription dynamically',
                        operationId: 'subscribe'
                    }
                }
            }
        },
        operations: {
            setup_kafka: kafkaMod.setup.bind(kafkaMod),
            produce: kafkaMod.produce.bind(kafkaMod),
            subscribe: kafkaMod.subscribe.bind(kafkaMod)
        },
        resources: [{
            uri: '/sys/queue/setup'
        }]
    };
};
