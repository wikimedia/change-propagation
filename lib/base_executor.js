"use strict";

const kafka = require('librdkafka-node');
const P = require('bluebird');
const uuid = require('cassandra-uuid').TimeUuid;
const HTTPError = require('hyperswitch').HTTPError;

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

class BaseExecutor {

    /**
     * Creates a new instance of a rule executor
     *
     * @param {Rule} rule
     * @param {KafkaConfig} kafkaConf
     * @param {Object} hyper
     * @param {function} log
     * @param {Object} options
     * @constructor
     */
    constructor(rule, kafkaConf, hyper, log, options) {
        if (this.constructor.name === 'BaseExecutor') {
            throw new Error('BaseService is abstract. Create Master or Worker instance.');
        }

        this.rule = rule;
        this.kafkaConf = kafkaConf;
        this.hyper = hyper;
        this.log = log;
        this.concurrency = rule.spec.concurrency || options.concurrency || DEFAULT_CONCURRENCY;

        this._commitTimeout = null;
        this._pendingTaskOffsets = [];
        this._toCommit = 0;
    }

    subscribe() {
        this.consumer = new kafka.KafkaConsumer(this.kafkaConf.consumerConf(`change-prop-${this.rule.name}`));
        this.consumer.subscribe([ this.subscribeTopic ]);
        for (let jobId = 0; jobId < this.concurrency; jobId++) {
            this._consume();
        }
    }

    close() {
        if (this._commitTimeout) {
            clearTimeout(this._commitTimeout);
            this._commitTimeout = null;
            this._doCommit();
        }
        this.consumer.close();
    }

    /** Private methods */

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

    _retryTopicName() {
        return `change-prop.retry.${this.rule.topic}`;
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

    _consume() {
        this.consumer.consume()
        .then((msg) => {
            this._pendingTaskOffsets.push(msg.offset);
            return this._safeParse(msg.payload.toString('utf8'))
            .then(this.onMessage.bind(this))
            .finally(() => {
                this._notifyFinished(msg.offset);
            });
        })
        .finally(this._consume.bind(this));
    }

    _notifyFinished(offset) {
        this._pendingTaskOffsets = this._pendingTaskOffsets.filter((o) => o === offset);
        if (this._pendingTaskOffsets.length) {
            this._toCommit = this._pendingTaskOffsets.sort()[0];
        } else {
            this._toCommit = offset + 1;
        }

        if (!this._commitTimeout) {
            this._commitTimeout = setTimeout(this._doCommit.bind(this),
                DEFAULT_COMMIT_INTERVAL);
        }
    }

    _doCommit() {
        const commit = new kafka.TopicPartition(this.subscribeTopic, 0, this._toCommit);
        this.consumer.commit([ commit ]);
        this._commitTimeout = null;
    }

    /** Abstract methods */
    get subscribeTopic() {
        throw new Error('Abstract');
    }

    onMessage(message) {
        throw new Error('Abstract');
    }

}

module.exports = BaseExecutor;