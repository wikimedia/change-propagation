"use strict";

const uuid = require('cassandra-uuid').TimeUuid;
const P = require('bluebird');

const utils = require('./utils');
const BaseExecutor = require('./base_executor');

/**
 * A rule executor managing matching and execution of a single rule
 */
class RetryExecutor extends BaseExecutor {
    /**
     * @inheritDoc
     */
    constructor(rule, kafkaConf, hyper, log, options) {
        super(rule, kafkaConf, hyper, log, options);
    }

    get subscribeTopic() {
        return `${this.kafkaFactory.consumeDC}.${this._retryTopicName()}`;
    }

    _delay(message) {
        const spec = this.rule.spec;
        const absoluteDelay = spec.retry_delay *
            Math.pow(spec.retry_factor, spec.retry_limit - message.retries_left);
        if (!message.meta.dt || !Date.parse(message.meta.dt)) {
            // No DT on the message, there's nothing we can do
            return P.delay(absoluteDelay);
        }
        const delayFromNow = (Date.parse(message.meta.dt) + absoluteDelay) - Date.now();
        if (delayFromNow > 0) {
            return P.delay(delayFromNow);
        } else {
            return P.resolve();
        }
    }

    onMessage(message, onExec) {
        if (!message) {
            // Don't retry if we can't parse an event, just log.
            return;
        }

        if (message.emitter_id !== this._emitterId()) {
            // Not our business, don't care
            return;
        }

        if (this._isLimitExceeded(message)) {
            // We've don our best, give up
            return;
        }

        const optionIndex = this._test(message.original_event);
        if (optionIndex === -1) {
            // doesn't match any more, possibly meaning
            // the rule has been changed since we last
            // executed it on the message
            return;
        }

        if (onExec) {
            onExec();
        }

        const statName = this.hyper.metrics.normalizeName(this.rule.name + '_retry');
        return this._delay(message)
        .then(() => this._exec(message.original_event,
            optionIndex, statName, new Date(message.meta.dt), message))
        .catch((e) => {
            e = BaseExecutor.decodeError(e);
            const retryMessage = this._constructRetryMessage(message.original_event,
                e, message.retries_left - 1, message);
            return this._catch(message, retryMessage, e);
        });
    }
}

module.exports = RetryExecutor;

