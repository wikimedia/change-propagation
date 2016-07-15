"use strict";

const P = require('bluebird');
const uuid = require('cassandra-uuid').TimeUuid;
const HTTPError = require('hyperswitch').HTTPError;
const TopicsNotExistError = require('wmf-kafka-node/lib/errors').TopicsNotExistError;
const Task = require('./task');
const utils = require('./utils');
const TaskQueue = require('../lib/task_queue');
const kafka = require('librdkafka-node');

/**
 * The default size of the task queue
 *
 * @type {number}
 * @const
 */
const DEFAULT_TASK_QUEUE_SIZE = 30;

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
    constructor(rule, kafkaFactory, hyper, log, options) {
        this.rule = rule;
        this.kafkaFactory = kafkaFactory;
        this.hyper = hyper;
        this.log = log;
        this.taskQueue = new TaskQueue({
            size: rule.spec.concurrency || options.concurrency || DEFAULT_TASK_QUEUE_SIZE
        });
    }

    /*_setConsumerLoggers(consumer, ruleName, topic) {
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
    }*/

    _test(event) {
        try {
            const optionIndex = this.rule.test(event);
            if (optionIndex === -1) {
                // no match, drop the message
                this.log(`debug/${this.rule.name}`, {
                    msg: 'Dropping event message', event: event
                });
            }
            return optionIndex;
        } catch (e) {
            this.log(`error/${this.rule.name}`, e);
            return -1;
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

    _exec(origEvent, optionIndex, statName, statDelayStartTime, retryEvent) {
        const rule = this.rule;

        this.log(`trace/${rule.name}`, { msg: 'Event message received', event: origEvent });

        // latency from the original event creation time to execution time
        this.hyper.metrics.endTiming([statName + '_delay'],
            statDelayStartTime || new Date(origEvent.meta.dt));

        const startTime = Date.now();
        const expander = {
            message: origEvent,
            match: rule.expand(optionIndex, origEvent)
        };
        return P.each(rule.getExec(optionIndex), (tpl) => {
            const request = tpl.expand(expander);
            request.headers = Object.assign(request.headers, {
                'x-request-id': origEvent.meta.request_id,
                'x-triggered-by': utils.triggeredBy(retryEvent || origEvent)
            });
            //console.log('REQUEST:', request);
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
            .tap(this._sampleLog.bind(this, retryEvent || origEvent, request))
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
        return `change-prop.retry.${this.rule.topic}`;
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

    _catch(message, retryMessage, e) {
        const reportError = () => this.hyper.post({
            uri: '/sys/queue/events',
            body: [this._constructErrorMessage(e, message)]
        });

        if (e.constructor.name !== 'HTTPError') {
            // We've got an error, but it's not from the update request, it's
            // some bug in change-prop. Log and send a fatal error message.
            this.hyper.log(`error/${this.rule.name}`, {
                message: 'Internal error in change-prop',
                error: e,
                event: message
            });
            return reportError();
        } else if (this.rule.shouldRetry(e)
            && !this._isLimitExceeded(retryMessage, e)) {
            return this._retry(retryMessage);
        } else if (!this.rule.shouldIgnoreError(e)) {
            return reportError();
        }
    }

    consumeRetry() {
        this.retryConsumer.consume()
        .then((msg) => {
            const statName = this.hyper.metrics.normalizeName(this.rule.name + '_retry');
            return this._safeParse(msg.payload.toString())
            .then((message) => {
                if (!message) {
                    // Don't retry if we can't parse an event, just log.
                    return this.consumeRetry();
                }

                if (message.emitter_id !== this._consumerId()) {
                    // Not our business, don't care
                    return this.consumeRetry();
                }

                if (this._isLimitExceeded(message)) {
                    // We've don our best, give up
                    return this.consumeRetry();
                }

                const optionIndex = this._test(message.original_event);
                if (optionIndex === -1) {
                    // doesn't match any more, possibly meaning
                    // the rule has been changed since we last
                    // executed it on the message
                    return this.consumeRetry();
                }

                return this.taskQueue.enqueue(new Task(this.retryConsumer,
                    `change-prop-retry-${this.rule.name}`,
                    message,
                    this._exec.bind(this, message.original_event,
                        optionIndex, statName, new Date(message.meta.dt), message),
                    (e) => {
                        e = decodeError(e);
                        const retryMessage = this._constructRetryMessage(message.original_event,
                            e, message.retries_left - 1, message);
                        return this._catch(message, retryMessage, e);
                    },
                    () => this.consumeRetry()
                ));
            });
        })
        .catch((err) => {
            // TODO:
            return this.consumeRetry();
        });
    }

    /**
     * Set's up a consumer a retry queue
     *
     * @private
     */
    _setUpRetryTopic() {
        this.retryConsumer = new kafka.KafkaConsumer({
            "default_topic_conf": {
                "auto.offset.reset": "largest"
            },
            "group.id": `change-prop-retry-${this.rule.name}`,
            // TODO: configure
            "metadata.broker.list": "127.0.0.1:9092",
            "fetch.wait.max.ms": "1",
            "fetch.min.bytes": "1",
            "queue.buffering.max.ms": "1"
        });
        this.retryConsumer.subscribe([ `${this.kafkaFactory.consumeDC}.${this._retryTopicName()}` ]);
        this.taskQueue.addConsumer(this.retryConsumer);
        this.consumeRetry();
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

    consume() {
        this.consumer.consume()
        .then((msg) => {

            const statName = this.hyper.metrics.normalizeName(this.rule.name);

            return this._safeParse(msg.payload.toString('utf8'))
            .then((message) => {
                if (!message) {
                    // no message we are done here
                    return this.consume();
                }

                const optionIndex = this._test(message);
                if (optionIndex === -1) {
                    // Message doesn't match any option for this rule, we're done
                    return this.consume();
                }

                return this.taskQueue.enqueue(new Task(
                    this.consumer,
                    `change-prop-${this.rule.name}`,
                    msg,
                    this._exec.bind(this, message, optionIndex, statName),
                    (e) => {
                        e = decodeError(e);
                        const retryMessage = this._constructRetryMessage(message, e);
                        return this._catch(message, retryMessage, e);
                    },
                    () => this.consume()
                ));
            });
        })
        .catch((err) => {
            // TODO
            this.consume()
        });
    }

    subscribe() {
        const rule = this.rule;
        this._setUpRetryTopic();
        this.consumer = new kafka.KafkaConsumer({
            "default_topic_conf": {
                "auto.offset.reset": "largest"
            },
            "group.id": `change-prop-${rule.name}`,
            // TODO: configure
            "metadata.broker.list": "127.0.0.1:9092",
            "fetch.wait.max.ms": "1",
            "fetch.min.bytes": "1",
            "queue.buffering.max.ms": "1"
        });
        this.consumer.subscribe([ `${this.kafkaFactory.consumeDC}.${rule.topic}` ]);
        this.taskQueue.addConsumer(this.consumer);
        // TODO: this is actually making it run one-by-one.
        this.consume();
        // TODO
        return P.resolve()
    /* TODO        .catch(TopicsNotExistError, (e) => {
            this.log('error/topic', e);
            // Exit async to let the logs get processed.
            setTimeout(() => process.exit(1), 100);
        });*/
    }

    close() {
   // TODO:     return this.taskQueue.close()
        //.then(this.retryConsumer.closeAsync.bind(this.retryConsumer))
        //.then(this.consumer.closeAsync.bind(this.consumer));
    }
}

module.exports = RuleExecutor;
