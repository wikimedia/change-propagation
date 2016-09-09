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
     *
     * @param message
     * @return {*|boolean}
     */
    getOptionIndex(message) {
        if (!message) {
            // no message we are done here
            return -1;
        }

        return this._test(message);
    }

    processMessage(message, optionIndex) {
        return this._exec(message, optionIndex)
        .catch((e) => {
            e = BaseExecutor.decodeError(e);
            const retryMessage = this._constructRetryMessage(message, e);
            return this._catch(message, retryMessage, e);
        });
    }
}

module.exports = RuleExecutor;
