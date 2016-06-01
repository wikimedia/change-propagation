"use strict";

const P = require('bluebird');
const uuid = require('cassandra-uuid').TimeUuid;
const HTTPError = require('hyperswitch').HTTPError;
const TopicsNotExistError = require('wmf-kafka-node/lib/errors').TopicsNotExistError;
const Task = require('./task');
const utils = require('./utils');

function decodeError(e) {
    if (Buffer.isBuffer(e.body)) {
        e.body = e.body.toString();
        try {
            e.body = JSON.parse(e.body);
        } catch (e) {
            // Not a JSON error
        }
    }
    return e;
}
/**
 * A rule executor managing matching and execution of a single rule
 */
class RuleExecutor {
    /**
     * Creates a new instance of a rule executor
     *
     * @param {Rule} rule
     * @param {KafkaFactory} kafkaFactory
     * @param {TaskQueue} taskQueue
     * @param {Object} hyper
     * @param {function} log
     * @constructor
     */
    constructor(rule, kafkaFactory, taskQueue, hyper, log) {
        this.rule = rule;
        this.kafkaFactory = kafkaFactory;
        this.taskQueue = taskQueue;
        this.hyper = hyper;
        this.log = log;
    }

    _setConsumerLoggers(consumer, ruleName, topic) {
        consumer.on('topics_changed', (topicList) => {
            // only one topic can be subscribed to by this client
            if (topicList && topicList.length) {
                this.log(`info/subscription/${ruleName}`, {
                    rule: { name: ruleName, topic },
                    msg: `Listening to ${topicList[0]}`
                });
                this._notifySubscribed(topic);
            } else {
                this.log(`info/subscription/${ruleName}`, {
                    rule: { name: ruleName, topic },
                    msg: `Lost ownership of ${topic}`
                });
            }
        });
        consumer.on('error', (err) => this.log(`warn/${ruleName}`, Object.assign(err, {
            rule: { name: ruleName, topic }
        })));
        return consumer;
    }

    _test(event) {
        try {
            if (this.rule.test(event)) {
                return true;
            }
            // no match, drop the message
            this.log(`debug/${this.rule.name}`, { msg: 'Dropping event message', event: event });
            return false;
        } catch (e) {
            this.log(`error/${this.rule.name}`, e);
            return false;
        }
    }

    _sampleLog(event, request, res) {
        const sampleLog = {
            message: 'Processed event sample',
            event: event,
            request: request,
            response: Object.assign({}, res)
        };
        // Don't include the full body in the log
        if (res.status === 200 || res.status === 201) {
            delete sampleLog.response.body;
        }
        this.hyper.log('trace/sample', sampleLog);
    }

    _exec(origEvent, statName, statDelayStartTime, retryEvent) {
        const rule = this.rule;

        this.log(`trace/${rule.name}`, { msg: 'Event message received', event: origEvent });

        // latency from the original event creation time to execution time
        this.hyper.metrics.endTiming([statName + '_delay'],
            statDelayStartTime || new Date(origEvent.meta.dt));

        const startTime = Date.now();
        const expander = {
            message: origEvent,
            match: rule.expand(origEvent)
        };
        return P.each(rule.exec, (tpl) => {
            const request = tpl.expand(expander);
            request.headers = Object.assign(request.headers, {
                'x-request-id': origEvent.meta.request_id,
                'x-triggered-by': utils.triggeredBy(retryEvent || origEvent)
            });
            return this.hyper.request(request)
            .tap((res) => {
                if (res.status === 301) {
                    // As we don't follow redirects, and we must always use normalized titles,
                    // receiving 301 indicates some error. Log a warning.
                    this.log(`warn/${this.rule.name}`, {
                        message: '301 redirect received, used a non-normalized title',
                        rule: this.rule.name,
                        event: origEvent
                    });
                }
            })
            .tap(this._sampleLog.bind(retryEvent || origEvent, request))
            .catch((e) => {
                this._sampleLog(retryEvent || origEvent, request, e);
                throw e;
            });
        })
        .finally(() => {
            this.hyper.metrics.endTiming([statName + '_exec'], startTime);
        });
    }

    _safeParse(message) {
        try {
            return P.resolve(JSON.parse(message));
        } catch (e) {
            this.log(`error/${this.rule.name}`, e);
            return this.hyper.post({
                uri: '/sys/queue/events',
                body: [ this._constructErrorMessage(e, message) ]
            })
            .thenReturn(undefined);
        }
    }

    _retryTopicName() {
        return 'change-prop.retry.' + this.rule.topic;
    }

    /**
     * Checks whether retry limit for this rule is exceeded.
     *
     * @param {Object} message a retry message to check
     * @param {Error} [e] optional Error that caused a retry
     * @returns {boolean}
     * @private
     */
    _isLimitExceeded(message, e) {
        if (message.retries_left <= 0) {
            this.log(`error/${this.rule.name}`, {
                message: 'Retry count exceeded',
                event: message,
                error: e
            });
            return true;
        }
        return false;
    }

    /**
     * Notify master what this worker is doing for debugging purposes if it fails
     * In production normally one worker will succeed subscribing only to one rule,
     * so this will emit the only rule the worker is processing
     *
     * @param {string} topic topic name this worker subscribed too
     * @private
     */
    _notifySubscribed(topic) {
        process.emit('service_status', {
            type: 'subscription',
            rule: this.rule.name,
            topic: topic
        });
    }

    /**
     * Set's up a consumer a retry queue
     *
     * @private
     */
    _setUpRetryTopic() {
        const retryTopicName = this._retryTopicName();

        return this.kafkaFactory.newConsumer(this.kafkaFactory.newClient(),
            retryTopicName,
            `change-prop-${retryTopicName}-${this.rule.name}`)
        .then((consumer) => {
            this.retryConsumer = this._setConsumerLoggers(consumer, this.rule.name, retryTopicName);
            this.retryConsumer.on('message', (msg) => {
                const statName = this.hyper.metrics.normalizeName(this.rule.name + '_retry');
                return this._safeParse(msg.value)
                .then((message) => {
                    if (!message) {
                        // Don't retry if we can't parse an event, just log.
                        return;
                    }

                    if (message.emitter_id !== this._consumerId()) {
                        // Not our business, don't care
                        return;
                    }

                    if (this._isLimitExceeded(message)) {
                        // We've don our best, give up
                        return;
                    }

                    if (!this._test(message.original_event)) {
                        // doesn't match any more, possibly meaning
                        // the rule has been changed since we last
                        // executed it on the message
                        return;
                    }

                    return this.taskQueue.enqueue(new Task(consumer, message,
                        this._exec.bind(this, message.original_event,
                            statName, new Date(message.meta.dt), message),
                        (e) => {
                            e = decodeError(e);
                            const retryMessage = this._constructRetryMessage(message.original_event,
                                e, message.retries_left - 1, message);
                            if (this.rule.shouldRetry(e)) {
                                if (this._isLimitExceeded(retryMessage, e)) {
                                    return this.hyper.post({
                                        uri: '/sys/queue/events',
                                        body: [this._constructErrorMessage(e, message)]
                                    });
                                }
                                return this._retry(retryMessage);
                            }
                        }
                    ));
                });
            });
        });
    }

    _retry(retryMessage) {
        const spec = this.rule.spec;
        const delay = spec.retry_delay *
            Math.pow(spec.retry_factor, spec.retry_limit - retryMessage.retries_left);
        return P.delay(delay)
        .then(() => this.hyper.post({
            uri: '/sys/queue/events',
            body: [ retryMessage ]
        }));
    }

    _consumerId() {
        return 'change-prop#' + this.rule.name;
    }

    _constructRetryMessage(event, errorRes, retriesLeft, retryEvent) {
        return {
            meta: {
                topic: this._retryTopicName(),
                schema_uri: 'retry/1',
                uri: event.meta.uri,
                request_id: event.meta.request_id,
                id: undefined, // will be filled later
                dt: undefined, // will be filled later
                domain: event.meta.domain
            },
            triggered_by: utils.triggeredBy(retryEvent || event),
            emitter_id: this._consumerId(),
            retries_left: retriesLeft === undefined ? this.rule.spec.retry_limit : retriesLeft,
            original_event: event,
            reason: errorRes && errorRes.body && errorRes.body.title
        };
    }

    /**
     * Create an error message for a special Kafka topic
     *
     * @param {Error} e an exception that caused a failure
     * @param {string|Object} event an original event. In case JSON parsing failed - it's a string.
     */
    _constructErrorMessage(e, event) {
        const eventUri = typeof event === 'string' ? '/error/uri' : event.meta.uri;
        const domain = typeof event === 'string' ? 'unknown' : event.meta.domain;
        const now = new Date();
        const errorEvent = {
            meta: {
                topic: 'change-prop.error',
                schema_uri: 'error/1',
                uri: eventUri,
                request_id: typeof event === 'string' ? undefined : event.meta.request_id,
                id: uuid.fromDate(now),
                dt: now.toISOString(),
                domain: domain
            },
            emitter_id: 'change-prop#' + this.rule.name,
            raw_event: typeof event === 'string' ? event : JSON.stringify(event),
            message: e.message,
            stack: e.stack
        };
        if (e.constructor === HTTPError) {
            errorEvent.details = {
                status: e.status,
                headers: e.headers,
                body: e.body
            };
        }
        return errorEvent;
    }

    subscribe() {
        const rule = this.rule;
        const client = this.kafkaFactory.newClient();
        return this._setUpRetryTopic()
        .then(() => {
            return this.kafkaFactory.newConsumer(client, rule.topic, `change-prop-${rule.name}`)
            .then((consumer) => {
                this.consumer = this._setConsumerLoggers(consumer, rule.name, rule.topic);
                this.taskQueue.addConsumer(this.consumer);
                this.consumer.on('message', (msg) => {
                    const statName = this.hyper.metrics.normalizeName(this.rule.name);
                    return this._safeParse(msg.value)
                    .then((message) => {
                        if (!message || !this._test(message)) {
                            // no message or no match, we are done here
                            return;
                        }

                        return this.taskQueue.enqueue(new Task(
                            consumer,
                            msg,
                            this._exec.bind(this, message, statName),
                            (e) => {
                                e = decodeError(e);
                                if (this.rule.shouldRetry(e)) {
                                    const retryMessage = this._constructRetryMessage(message, e);
                                    // Retry limit might be 0,
                                    // so check if we need to post a retry message at all.
                                    if (this._isLimitExceeded(retryMessage, e)) {
                                        return this.hyper.post({
                                            uri: '/sys/queue/events',
                                            body: [this._constructErrorMessage(e, message)]
                                        });
                                    }
                                    return this._retry(retryMessage);
                                }
                            }
                        ));
                    });
                });
            })
            .catch(TopicsNotExistError, (e) => {
                this.log('error/topic', e);
                // Exit async to let the logs get processed.
                setTimeout(() => process.exit(1), 100);
            });
        });
    }
}

module.exports = RuleExecutor;
