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

    onMessage(message, onExec) {
        if (!message) {
            // no message we are done here
            return;
        }

        const optionIndex = this._test(message);
        if (optionIndex === -1) {
            // Message doesn't match any option for this rule, we're done
            return;
        }

        if (onExec) {
            onExec();
        }

        const statName = this.hyper.metrics.normalizeName(this.rule.name);
        return this._exec(message, optionIndex, statName)
        .catch((e) => {
            e = BaseExecutor.decodeError(e);
            const retryMessage = this._constructRetryMessage(message, e);
            return this._catch(message, retryMessage, e);
        });
    }
}

module.exports = RuleExecutor;
