"use strict";

const BaseExecutor = require('./base_executor');

/**
 * A rule executor managing matching and execution of a single rule
 */
class RuleExecutor extends BaseExecutor {
    /**
     * @inheritDoc
     */
    constructor(rule, kafkaConf, hyper, log, options) {
        super(rule, kafkaConf, hyper, log, options);
    }

    get subscribeTopic() {
        return `${this.kafkaFactory.consumeDC}.${this.rule.topic}`;
    }

    get statName() {
        return this.hyper.metrics.normalizeName(this.rule.name);
    }

    /**
     * Returns a handler to be used or undefined.
     *
     * @param message
     * @return {*|boolean}
     */
    getHandler(message) {
        if (!message) {
            // no message we are done here
            return undefined;
        }
        const handlerIndex = this._test(message);
        if (handlerIndex === -1) {
            return undefined;
        }
        return this.rule.getHandler(handlerIndex);
    }

    processMessage(message, hander) {
        return this._exec(message, hander)
        .catch((e) => {
            e = BaseExecutor.decodeError(e);
            const retryMessage = this._constructRetryMessage(message, e);
            return this._catch(message, retryMessage, e);
        });
    }
}

module.exports = RuleExecutor;
