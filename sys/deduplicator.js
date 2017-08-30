"use strict";

const mixins = require('../lib/mixins');
const utils = require('../lib/utils');

const DUPLICATE = { status: 200, body: true };
const NOT_DUPLICATE = { status: 200, body: false };

class Deduplicator extends mixins.mix(Object).with(mixins.Redis) {
    constructor(options) {
        super(options);

        this._options = options;
        this._log = this._options.log || (() => {});
        this._expire_timeout = options.window || 86400;
    }

    /**
     * Checks whether the message is a duplicate
     * @param {HyperSwitch} hyper
     * @param {Object} req
     * @return {Promise} response status shows whether it's a duplicate or not.
     */
    checkDuplicate(hyper, req) {
        const name = req.params.name;
        const message = req.body;

        // First, look at the individual event duplication
        const messageKey = `CP_dedupe_${name}_${message.meta.id}`;
        return this._redis.setnxAsync(messageKey, '1')
        // Expire the key or renew the expiration timestamp if the key existed
        .tap(() => this._redis.expireAsync(messageKey, Math.ceil(this._expire_timeout / 24)))
        // If that key already existed - that means it's a duplicate
        .then((setResult) => {
            if (setResult) {
                return NOT_DUPLICATE;
            }
            hyper.metrics.increment(`${name}_dedupe`);
            hyper.log('trace/dedupe', {
                message: 'Event was deduplicated based on id',
                event_str: utils.stringify(message),
            });
            return DUPLICATE;
        })
        .then((individualDeduplicated) => {
            if (individualDeduplicated.body || !message.root_event) {
                // If the message was individually deduped or if it has no root event info,
                // don't use deduplication by the root event
                return individualDeduplicated;
            }

            const rootEventKey = `CP_dedupe_${name}_${message.root_event.signature}`;
            return this._redis.getAsync(rootEventKey)
            .then((oldEventTimestamp) => {
                if (oldEventTimestamp
                        && new Date(oldEventTimestamp) > new Date(message.root_event.dt)) {
                    hyper.metrics.increment(`${name}_dedupe`);
                    hyper.log('trace/dedupe', {
                        message: 'Event was deduplicated based on root event',
                        event_str: utils.stringify(message),
                        signature: message.root_event.signature,
                        newer_dt: oldEventTimestamp
                    });
                    return DUPLICATE;
                }
                return this._redis.setAsync(rootEventKey, message.root_event.dt)
                .then(() => this._redis.expireAsync(rootEventKey, this._expire_timeout))
                .thenReturn(NOT_DUPLICATE);
            });
        })
        .catch((e) => {
            this._log('error/dedupe', {
                message: 'Error during deduplication',
                error: e
            });
            return NOT_DUPLICATE;
        });
    }
}

module.exports = (options) => {
    const ps = new Deduplicator(options);

    return {
        spec: {
            paths: {
                '/{name}': {
                    post: {
                        operationId: 'checkDuplicate'
                    }
                }
            }
        },
        operations: {
            checkDuplicate: ps.checkDuplicate.bind(ps)
        }
    };
};
