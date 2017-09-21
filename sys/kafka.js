"use strict";


/**
 * restbase-mod-queue-kafka main entry point
 */


const P = require('bluebird');
const HyperSwitch = require('hyperswitch');
const HTTPError = HyperSwitch.HTTPError;
const uuid = require('cassandra-uuid').TimeUuid;
const Budgeteer = require('budgeteer');

const utils = require('../lib/utils');
const Rule = require('../lib/rule');
const KafkaFactory = require('../lib/kafka_factory');
const RuleSubscriber = require('../lib/rule_subscriber');

class Kafka {
    constructor(options) {
        this.options = options;
        this.staticRules = options.templates || {};
        this._services = {
            log: options.log || (() => { }),
            kafka: new KafkaFactory(options),
            // Rate limiting and deduplication
            budgeteer: new Budgeteer(options.budgeteer),
            hyper: null // Filled in during setup
        };

        // Shorthands
        this.log = this._services.log;


        this.subscriber = new RuleSubscriber(options, this._services);
        HyperSwitch.lifecycle.on('close', () => this.subscriber.unsubscribeAll());
    }

    setup(hyper) {
        this._services.hyper = hyper;
        return this._services.kafka.createGuaranteedProducer(this.log)
        .then((producer) => {
            this.producer = producer;
            HyperSwitch.lifecycle.on('close', () => P.all([
                this.producer.disconnect(),
                this._services.budgeteer.close()
            ]));
            return this._subscribeRules(this.staticRules);
        })
        .tap(() => this.log('info/change-prop/init', 'Kafka Queue module initialised'));
    }

    _subscribeRules(rules) {
        const activeRules = Object.keys(rules)
            .map(ruleName => new Rule(ruleName, rules[ruleName]))
            .filter(rule => !rule.noop);

        return P.each(activeRules, rule =>
            this.subscriber.subscribe(rule))
        .thenReturn({ status: 201 });
    }

    subscribe(req) {
        return this._subscribeRules(req.body);
    }

    produce(hyper, req) {
        if (this.options.test_mode) {
            this.log('trace/produce', 'Running in TEST MODE; Production disabled');
            return { status: 201 };
        }

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
            message.meta.id = message.meta.id || uuid.fromDate(now).toString();
            message.meta.dt = message.meta.dt || now.toISOString();
            message.meta.request_id = message.meta.request_id || utils.requestId();
            if (!message || !message.meta || !message.meta.topic) {
                throw new HTTPError({
                    status: 400,
                    body: {
                        type: 'bad_request',
                        detail: 'Event must have a meta.topic and meta.id properties',
                        event_str: message
                    }
                });
            }
        });
        return P.all(messages.map((message) => {
            const topicName = message.meta.topic.replace(/\./g, '_');
            hyper.metrics.increment(`produce_${hyper.metrics.normalizeName(topicName)}`);

            return this.producer.produce(
                `${this._services.kafka.produceDC}.${message.meta.topic}`, 0,
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
                '/events': {
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
