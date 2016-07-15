"use strict";


/**
 * restbase-mod-queue-kafka main entry point
 */


const P = require('bluebird');
const HyperSwitch = require('hyperswitch');
const HTTPError = HyperSwitch.HTTPError;
const uuid = require('cassandra-uuid').TimeUuid;

const Rule = require('../lib/rule');
const KafkaFactory = require('../lib/kafka_factory');
const RuleExecutor = require('../lib/rule_executor');
const kafka = require('librdkafka-node');

class Kafka {
    constructor(options) {
        this.options = options;
        this.log = options.log || function() { };
        this.kafkaFactory = new KafkaFactory({
            uri: options.uri || 'localhost:2181/',
            clientId: options.client_id || 'change-propagation',
            consume_dc: options.consume_dc,
            produce_dc: options.produce_dc,
            dc_name: options.dc_name
        });
        this.staticRules = options.templates || {};
        this.ruleExecutors = {};

        HyperSwitch.lifecycle.on('close', () => {
            this.close();
        });
    }

    setup(hyper) {
        this.producer = new kafka.Producer({
            "metadata.broker.list": "127.0.0.1:9092",
            "queue.buffering.max.ms": "1"
        });
        // TODO: it's not a promise actually
        return this._subscribeRules(hyper, this.staticRules)
        .tap(() => this.log('info/change-prop/init', 'Kafka Queue module initialised'));
    }

    _subscribeRules(hyper, rules) {
        const activeRules = Object.keys(rules)
            .map((ruleName) => new Rule(ruleName, rules[ruleName]))
            .filter((rule) => !rule.noop);
        return P.each(activeRules, (rule) => {
            this.ruleExecutors[rule.name] = new RuleExecutor(rule,
                this.kafkaFactory, hyper, this.log, this.options);
            return this.ruleExecutors[rule.name].subscribe();
        })
        .thenReturn({ status: 201 });
    }

    subscribe(hyper, req) {
        return this._subscribeRules(hyper, req.body);
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
            return this.producer.produce(
                `${this.kafkaFactory.produceDC}.${message.meta.topic}`,
                JSON.stringify(message)
            );
        }))
        .thenReturn({ status: 201 });
    }

    close() {
        return P.each(Object.values(this.ruleExecutors),
            (executor) => executor.close())
        .thenReturn({ status: 200 });
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
