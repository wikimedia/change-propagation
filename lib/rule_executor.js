"use strict";

const BaseExecutor = require('./base_executor');
const URI = require('hyperswitch').URI;

/**
 * A rule executor managing matching and execution of a single rule
 */
class RuleExecutor extends BaseExecutor {
    get subscribeTopics() {
        return this.rule.topics.map(topic => `${this.kafkaFactory.consumeDC}.${topic}`);
    }

    statName(topic) {
        return this.hyper.metrics.normalizeName(
            `${this.rule.name}-${topic.replace('.', '_')}`);
    }

    /**
     * Returns a handler to be used or undefined.
     * @param {Object} message the message to process
     * @return {Function|boolean}
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

    _dedupeMessage(expander) {
        return this.hyper.post({
            uri: new URI(`/sys/dedupe/${this.rule.name}`),
            body: expander.message
        })
        .get('body')
        .catch({ status: 404 }, () => false);
    }
}

module.exports = RuleExecutor;
