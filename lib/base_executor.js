"use strict";

const P = require('bluebird');
const uuid = require('cassandra-uuid').TimeUuid;
const HTTPError = require('hyperswitch').HTTPError;
const URI = require('hyperswitch').URI;
const kafka = require('node-rdkafka');

const utils = require('./utils');

/**
 * The default number of tasks that could be run concurrently
 *
 * @type {number}
 * @const
 */
const DEFAULT_CONCURRENCY = 30;

/**
 * The default maximum delay to commit the offsets
 *
 * @const
 * @type {number}
 */
const DEFAULT_COMMIT_INTERVAL = 500;

/**
 * The default number of messages to consume at once
 * For low-volume topics this must be one to avoid big delays in consumption.
 *
 * @const
 * @type {number}
 */
const DEFAULT_CONSUMER_BATCH_SIZE = 1;

class BaseExecutor {

    /**
     * Creates a new instance of a rule executor
     * @param {Rule} rule
     * @param {KafkaFactory} kafkaFactory
     * @param {Object} hyper
     * @param {Function} log
     * @param {Object} options
     * @class
     */
    constructor(rule, kafkaFactory, hyper, log, options) {
        if (this.constructor.name === 'BaseExecutor') {
            throw new Error('BaseService is abstract. Create Master or Worker instance.');
        }

        this.rule = rule;
        this.kafkaFactory = kafkaFactory;
        this.hyper = hyper;
        this.log = log;
        this.options = options;
        this.concurrency = rule.spec.concurrency || this.options.concurrency || DEFAULT_CONCURRENCY;
        this.consumerBatchSize = rule.spec.consumer_batch_size
            || this.options.consumer_batch_size
            || DEFAULT_CONSUMER_BATCH_SIZE;

        this._commitTimeout = null;
        // In order ti filter out the pending messages faster make them offset->msg map
        this._pendingMsgs = new Map();
        this._consuming = false;
    }

    subscribe() {
        const prefix = this.options.test_mode ? `test-change-prop` : `change-prop`;
        return this.kafkaFactory.createConsumer(
            `${prefix}-${this.rule.name}`,
            this.subscribeTopics,
            this.hyper.metrics
        )
        .then((consumer) => {
            this.consumer = consumer;
            P.delay(this.kafkaFactory.startup_delay).then(() => this._consume());
        });
    }

    _safeParse(payload) {
        try {
            return JSON.parse(payload);
        } catch (e) {
            this.log(`error/${this.rule.name}`, e);
            this.hyper.post({
                uri: new URI('/sys/queue/events'),
                body: [ this._constructErrorMessage(e, payload) ]
            });
        }
    }

    _consume() {
        this._consuming = true;
        this.consumer.consumeAsync(this.consumerBatchSize)
        .then((messages) => {
            if (!messages.length) {
                // No new messages, delay a bit and try again.
                return P.delay(100);
            }

            messages.forEach((msg) => {
                const message = this._safeParse(msg.value.toString('utf8'));

                this.hyper.metrics.increment(
                    `${this.statName(message.meta.topic)}_dequeue`,
                    1,
                    0.1);

                const handler = this.getHandler(message);
                if (handler) {
                    // We're pushing it to pending messages only if it matched so that items
                    // that don't match don't count against the concurrency limit.
                    this._pendingMsgs.set(msg.offset, msg);
                    // Note: we don't return the promise here since we wanna process messages
                    // asynchronously from consuming them to be able to fill up the pendingMsg
                    // queue and achieve the level of concurrency we want.
                    this.processMessage(message, handler)
                    .finally(() => {
                        this._notifyFinished(msg);
                        if (this._pendingMsgs.size < this.concurrency && !this._consuming) {
                            this._consume();
                        }
                    });
                }
            });
        })
        .catch((e) => {
            // This errors must come from the KafkaConsumer
            // since the actual handler must never throw errors
            /* eslint-disable indent */
            switch (e.code) {
                case kafka.CODES.ERRORS.ERR__PARTITION_EOF:
                case kafka.CODES.ERRORS.ERR__TIMED_OUT:
                    // We're reading to fast, nothing is there, slow down a little bit
                    return P.delay(100);
                default:
                    this.log(`error/consumer/${this.rule.name}`, e);
                    if (e.code === kafka.CODES.ERRORS.ERR__STATE) {
                        // KafkaConsumer is disconnected or entered error state.
                        // Give it some time to reconnect before the new attempt
                        // to fetch messages again to avoid a tight loop
                        return P.delay(1000);
                    }
            }
            /* eslint-enable indent */
        })
        .finally(() => {
            if (this._pendingMsgs.size < this.concurrency) {
                this._consume();
            } else {
                this._consuming = false;
            }
        });
    }

    /**
     * Checks whether a message should be rate-limited
     * @param {Object} expander the limiter key expander
     * @return {Promise<boolean>}
     * @private
     */
    _rateLimitMessage(expander) {
        if (!this.rule.getRateLimiterTypes().length) {
            return P.resolve(false);
        }
        return P.all(this.rule.getRateLimiterTypes().map((type) => {
            const key = this.rule.getLimiterKey(type, expander);
            return this.hyper.get({
                uri: new URI(`/sys/limit/${type}/${key}`)
            });
        }))
        .thenReturn(false)
        .catch({ status: 404 }, () => {
            // If the limiter module is not configured, ignore
            return false;
        })
        // Will throw if any of the limiters failed,
        // the error message will say which one is failed.
        .catch({ status: 429 }, (e) => {
            this.log('error/ratelimit', () => ({
                msg: 'Rate-limited message processing',
                rule_name: this.rule.name,
                limiter: e.body.message,
                limiter_key: e.body.key,
                event_str: utils.stringify(expander.message)
            }));
            return true;
        });
    }

    _updateLimiters(expander, status) {
        if (!this.rule.getRateLimiterTypes().length) {
            // No limiters configured, don't care.
            return;
        }
        return P.each(this.rule.getRateLimiterTypes(), (type) => {
            // TODO: calculate the cost function and actually POST the cost!

            if (status >= 500) {
                const limiterKey = this.rule.getLimiterKey(type, expander);
                return this.hyper.post({
                    uri: new URI(`/sys/limit/${type}/${limiterKey}`)
                }).catch({ status: 429 }, () => {
                    // No need to react here, we'll reject the next message
                }).catch({ status: 404 }, () => {
                    // If the limiter module is not configured, ignore
                });
            }
        });
    }

    _notifyFinished(finishedMsg) {
        this._pendingMsgs.delete(finishedMsg.offset);

        if (this.options.test_mode) {
            this.log('trace/commit', 'Running in TEST MODE; Offset commits disabled');
            return;
        }

        if (!this._commitTimeout) {
            this._commitTimeout = setTimeout(() => {
                this._commitTimeout = null;
                let committing = finishedMsg;
                // Not check if any of the pending messages have offset lower then finished msg
                if (this._pendingMsgs.size) {
                    for (const [offset, message] of this._pendingMsgs.entries()) {
                        if (offset < committing.offset) {
                            committing = message;
                        }
                    }
                }
                return this.consumer.commitMessageAsync(committing)
                .catch((e) => {
                    this.log(`error/commit/${this.rule.name}`, () => ({
                        msg: 'Commit failed',
                        offset: committing.offset,
                        raw_event: committing.value.toString(),
                        description: e.toString()
                    }));
                });
            }, DEFAULT_COMMIT_INTERVAL);
        }
    }


    /** Private methods */

    _test(event) {
        try {
            const optionIndex = this.rule.test(event);
            if (optionIndex === -1) {
                // no match, drop the message
                this.log(`debug/${this.rule.name}`, () => ({
                    msg: 'Dropping event message',
                    event_str: utils.stringify(event)
                }));
            }
            return optionIndex;
        } catch (e) {
            this.log(`error/${this.rule.name}`, e);
            return -1;
        }
    }

    _sampleLog(event, request, res) {
        const sampleLog = () => {
            const log = {
                message: 'Processed event sample',
                event_str: utils.stringify(event),
                request: {
                    uri: `${request.uri}`,
                    headers: request.headers
                },
                response: {
                    status: res.status,
                    headers: res.headers,
                    body: utils.stringify(request.body).substr(0, 1024)
                }
            };
            // Don't include the full body in the log
            if (res.status >= 400) {
                log.response.body = BaseExecutor.decodeError(res).body;
            }
            return log;
        };
        this.hyper.log('trace/sample', sampleLog);
    }

    _exec(origEvent, handler, statDelayStartTime, retryEvent) {
        const rule = this.rule;
        const startTime = Date.now();

        const expander = {
            message: origEvent,
            match: handler.expand(origEvent)
        };

        if (origEvent.delay_until) {
            // The delay_until in the job schema is a timestamps in seconds
            const delayUntil = origEvent.delay_until * 1000;
            const now = Date.now();
            if (delayUntil > Date.now()) {
                const timeLeft = delayUntil - now;
                if (timeLeft > 20000) {
                    // Not ready to execute yet - delay for some time and put back to the queue
                    return P.delay(20000)
                    .then(() => {
                        return this.hyper.post({
                            uri: new URI('/sys/queue/events'),
                            body: [ origEvent ]
                        });
                    });
                }
                // The exec time is to soon to re-enqueue - just wait and execute
                return P.delay(timeLeft + 1)
                .then(() => this._exec(origEvent, handler, statDelayStartTime, retryEvent));
            }
        }

        if (handler.sampler && !handler.sampler.accept(expander)) {
            this.log(`trace/${rule.name}`, () => ({
                msg: 'Disregarding event; Filtered by sampler',
                event: utils.stringify(origEvent)
            }));
            return P.resolve({ status: 200 });
        }

        if (this.rule.shouldAbandon(origEvent)) {
            this.log('trace/abandon', {
                message: 'Event was abandoned',
                event_str: utils.stringify(origEvent),
            });
            return P.resolve({ status: 200 });
        }

        this.log(`trace/${rule.name}`, () => ({
            msg: 'Event message received',
            event_str: utils.stringify(origEvent) }));

        // Latency from the event (if it's a retry - an original event)
        // creation time to execution time
        this.hyper.metrics.endTiming([`${this.statName(origEvent.meta.topic)}_delay`],
            statDelayStartTime || new Date(origEvent.meta.dt));

        // This metric doesn't make much sense for retries
        if (origEvent.root_event && !retryEvent) {
            // Latency from the beginning of the recursive event chain
            // to the event execution
            this.hyper.metrics.endTiming([`${this.statName(origEvent.meta.topic)}_totaldelay`],
                new Date(origEvent.root_event.dt));
        }

        const redirectCheck = (res) => {
            if (res.status === 301) {
                // As we don't follow redirects, and we must use normalized titles,
                // receiving 301 indicates some error. Log a warning.
                this.log(`warn/${this.rule.name}`, () => ({
                    message: '301 redirect received, used a non-normalized title',
                    rule: this.rule.name,
                    event_str: utils.stringify(origEvent)
                }));
            }
        };

        return this._rateLimitMessage(expander)
        .then((isRateLimited) => {
            if (isRateLimited) {
                return { status: 200 };
            }
            return this._dedupeMessage(expander)
            .then((messageDeduped) => {
                if (messageDeduped) {
                    return { status: 200 };
                }
                return P.each(handler.exec, (tpl) => {
                    const request = tpl.expand(expander);
                    request.headers = Object.assign(request.headers, {
                        'x-request-id': origEvent.meta.request_id,
                        'x-triggered-by': utils.triggeredBy(retryEvent || origEvent)
                    });
                    return this.hyper.request(request)
                    .tap(redirectCheck)
                    .catch((e) => {
                        if (this.rule.shouldIgnoreError(BaseExecutor.decodeError(e))) {
                            return { status: 200 };
                        }
                        throw e;
                    })
                    .tap(this._sampleLog.bind(this, retryEvent || origEvent, request))
                    .tapCatch(this._sampleLog.bind(this, retryEvent || origEvent, request));
                })
                .tap(() => this._updateLimiters(expander, 200))
                .tapCatch(e => this._updateLimiters(expander, e.status))
                .finally(() => this.hyper.metrics.endTiming(
                    [`${this.statName(origEvent.meta.topic)}_exec`],
                    startTime)
                );
            });
        });
    }

    retryTopicName(topic) {
        return `${this.hyper.config.service_name}.retry.${topic}`;
    }

    emitterId() {
        return `${this.hyper.config.service_name}#${this.rule.name}`;
    }

    // TODO: Remove once the switch is completed
    _oldEmitterId() {
        return `change-prop#${this.rule.name}`;
    }

    _errorTopicName() {
        return `${this.hyper.config.service_name}.error`;
    }

    _constructRetryMessage(event, errorRes, retriesLeft, retryEvent) {
        const result = {
            meta: {
                topic: this.retryTopicName(event.meta.topic),
                schema_uri: 'retry/1',
                uri: event.meta.uri,
                domain: event.meta.domain
            },
            triggered_by: utils.triggeredBy(retryEvent || event),
            emitter_id: this.emitterId(),
            retries_left: retriesLeft === undefined ? this.rule.spec.retry_limit : retriesLeft,
            original_event: event,
            error_status: errorRes && errorRes.status
        };
        if (errorRes && errorRes.body && typeof errorRes.body === 'object') {
            result.reason = (errorRes.body.title || errorRes.body.message);
        } else if (errorRes && errorRes.body && typeof errorRes.body === 'string') {
            // Sometimes MediaWiki will send us error body as HTML,
            // for PHP fatals or 503 for example. Record it for easier debugging.
            result.reason = errorRes.body;
        }
    }

    /**
     * Checks whether retry limit for this rule is exceeded.
     * @param {Object} message a retry message to check
     * @param {Error} [e] optional Error that caused a retry
     * @return {boolean}
     * @private
     */
    _isLimitExceeded(message, e) {
        if (message.retries_left <= 0) {
            this.log(`warn/${this.rule.name}`, () => ({
                message: 'Retry count exceeded',
                event_str: utils.stringify(message),
                status: e.status,
                page: message.meta.uri,
                description: e.message,
                stack: e.stack
            }));
            return true;
        }
        return false;
    }

    _catch(message, retryMessage, e) {
        const reportError = () => this.hyper.post({
            uri: new URI('/sys/queue/events'),
            body: [this._constructErrorMessage(e, message)]
        });

        if (e.constructor.name !== 'HTTPError') {
            // We've got an error, but it's not from the update request, it's
            // some bug in change-prop. Log and send a fatal error message.
            this.hyper.log(`fatal/${this.rule.name}`, () => ({
                message: `Internal error in ${this.hyper.config.service_name}`,
                description: `${e}`,
                stack: e.stack,
                event_str: utils.stringify(message)
            }));
            return reportError();
        }

        if (!this.rule.shouldIgnoreError(e)) {
            if (this.rule.shouldRetry(e)
                && !this._isLimitExceeded(retryMessage, e)) {
                return this.hyper.post({
                    uri: new URI('/sys/queue/events'),
                    body: [ retryMessage ]
                });
            }
            return reportError();
        }
    }


    /**
     * Create an error message for a special Kafka topic
     * @param {Error} e an exception that caused a failure
     * @param {string|Object} event an original event. In case JSON parsing failed - it's a string.
     */
    _constructErrorMessage(e, event) {
        const genericErrorURI = 'https://mediawiki.org/wiki/ChangePropagation/error';
        const eventUri = typeof event === 'string' ? genericErrorURI : event.meta.uri;
        const domain = typeof event === 'string' ? 'unknown' : event.meta.domain;
        const now = new Date();
        const errorEvent = {
            meta: {
                topic: this._errorTopicName(),
                schema_uri: 'error/1',
                uri: eventUri,
                id: uuid.fromDate(now),
                dt: now.toISOString(),
                domain
            },
            emitter_id: this.emitterId(),
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

    close() {
        this.consumer.disconnect();
    }

    static decodeError(e) {
        if (Buffer.isBuffer(e.body)) {
            e.body = e.body.toString();
            try {
                e.body = JSON.parse(e.body);
            } catch (err) {
                // Not a JSON error
            }
        }
        return e;
    }
}

module.exports = BaseExecutor;
