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
const TaskQueue = require('../lib/task_queue');

class Kafka {
    constructor(options) {
        this.tqOpts = { size: options.concurrency };
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
        this.taskQueue = new TaskQueue(this.tqOpts);

        HyperSwitch.lifecycle.on('close', () => {
            this.close();
        });
    }

    setup(hyper) {
        return this.kafkaFactory.newProducer(this.kafkaFactory.newClient())
        .then((producer) => {
            this.producer = producer;
            return this._subscribeRules(hyper, this.staticRules);
        })
        .tap(() => this.log('info/change-prop/init', 'Kafka Queue module initialised'));
    }

    _subscribeRules(hyper, rules) {
        const activeRules = Object.keys(rules)
            .map((ruleName) => new Rule(ruleName, rules[ruleName]))
            .filter((rule) => !rule.noop);
        return P.each(activeRules, (rule) => {
            this.ruleExecutors[rule.name] = new RuleExecutor(rule,
                this.kafkaFactory, this.taskQueue, hyper, this.log);
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
        const groupedPerTopic = messages.reduce((result, message) => {
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
            const topic = message.meta.topic;
            result[topic] = result[topic] || [];
            const now = new Date();
            message.meta.id = message.meta.id || uuid.fromDate(now).toString();
            message.meta.dt = message.meta.dt || now.toISOString();
            result[topic].push(JSON.stringify(message));
            return result;
        }, {});

        return this.producer.sendAsync(Object.keys(groupedPerTopic).map((topic) => {
            return {
                topic: `${this.kafkaFactory.produceDC}.${topic}`,
                messages: groupedPerTopic[topic]
            };
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
