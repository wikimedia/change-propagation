"use strict";


/**
 * restbase-mod-queue-kafka main entry point
 */


const P = require('bluebird');
const HyperSwitch = require('hyperswitch');
const HTTPError = HyperSwitch.HTTPError;
const uuid = require('cassandra-uuid').TimeUuid;

const Rule = require('../lib/rule');
const KafkaConfig = require('../lib/kafka_factory');
const RuleExecutor = require('../lib/rule_executor');
const RetryExecutor = require('../lib/retry_executor');

class Kafka {
    constructor(options) {
        this.options = options;
        this.log = options.log || function() { };
        this.kafkaFactory = new KafkaConfig(options);
        this.staticRules = options.templates || {};
        this.ruleExecutors = {};
    }

    setup(hyper) {
        return this.kafkaFactory.createProducer()
        .then((producer) => {
            this.producer = producer;
            return this._subscribeRules(hyper, this.staticRules);
        })
        .tap(() => this.log('info/change-prop/init', 'Kafka Queue module initialised'))
        .thenReturn({ status: 201 });
    }

    _subscribeRules(hyper, rules) {
        const activeRules = Object.keys(rules)
            .map((ruleName) => new Rule(ruleName, rules[ruleName]))
            .filter((rule) => !rule.noop);
        return P.all(activeRules.map( (rule) => {
            this.ruleExecutors[rule.name] = new RuleExecutor(rule,
                this.kafkaFactory, hyper, this.log, this.options);
            this.ruleExecutors[`${rule.name}_retry`] = new RetryExecutor(rule,
                this.kafkaFactory, hyper, this.log, this.options);
            return P.join(
                    this.ruleExecutors[rule.name].subscribe(),
                    this.ruleExecutors[`${rule.name}_retry`].subscribe());
        }));
    }

    subscribe(hyper, req) {
        return this._subscribeRules(hyper, req.body).thenReturn({ status: 201 });
    }

    produce(hyper, req) {
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
            if (!message || !message.meta || !message.meta.topic) {
                throw new HTTPError({
                    status: 400,
                    body: {
                        type: 'bad_request',
                        detail: 'Event must have a meta.topic property',
                        event: message
                    }
                });
            }
        });
        return P.all(messages.map((message) => {
            const now = new Date();
            message.meta.id = message.meta.id || uuid.fromDate(now).toString();
            message.meta.dt = message.meta.dt || now.toISOString();
            return this.producer.produceAsync({
                message: JSON.stringify(message),
                topic: `${this.kafkaFactory.produceDC}.${message.meta.topic}`,
            });
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
