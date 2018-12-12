"use strict";

const mixins = require('../lib/mixins');
const utils = require('../lib/utils');

const DUPLICATE = { status: 200, body: true };
const NOT_DUPLICATE = { status: 200, body: false };

class Deduplicator extends mixins.mix(Object).with(mixins.Redis) {
    constructor(options) {
        super(options);

        this._options = options || {};
        this._expire_timeout = options.window || 86400;
        this._prefix = this._options.redis_prefix || 'CP';
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

        // First, look at the individual event duplication based on ID
        // This happens when we restart ChangeProp and reread some of the
        // exact same events which were executed but was not committed.
        const messageKey = `${this._prefix}_dedupe_${name}_${message.meta.id}`;
        return this._redis.setnxAsync(messageKey, '1')
        // Expire the key or renew the expiration timestamp if the key existed
        .tap(() => this._redis.expireAsync(messageKey, Math.ceil(this._expire_timeout / 24)))
        // If that key already existed - that means it's a duplicate
        .then((setResult) => {
            if (setResult) {
                return NOT_DUPLICATE;
            }
            hyper.metrics.increment(`${name}_dedupe`);
            hyper.logger.log('trace/dedupe', () => ({
                message: 'Event was deduplicated based on id',
                event_str: utils.stringify(message),
            }));
            return DUPLICATE;
        })
        .then((individualDuplicate) => {
            if (individualDuplicate.body || !message.sha1) {
                // If the message was sha1-deduped or if it has no root event info,
                // don't use deduplication by the root event
                return individualDuplicate;
            }
            const messageKey = `${this._prefix}_dedupe_${name}_${message.sha1}`;
            return this._redis.getAsync(messageKey)
            .then((previousExecutionTime) => {
                // If the same event (by sha1) was created before the previous execution
                // time, the changes that caused it were already in the database, so it
                // will be a no-op and can be deduplicated.
                if (previousExecutionTime
                        // Give that the resolution of the event dt is 1 second, this could
                        // be false-positive when the job queue is so quick that it executes
                        // two jobs with the same SHA1 within a single second. To be on the safe
                        // side - subtract 1 second from the previous execution time to allow for
                        // some lag.
                        && new Date(previousExecutionTime) - 1000 > new Date(message.meta.dt)) {
                    hyper.metrics.increment(`${name}_dedupe`);
                    hyper.logger.log('trace/dedupe', () => ({
                        message: 'Event was deduplicated based on sha1',
                        event_str: utils.stringify(message),
                        newer_dt: previousExecutionTime
                    }));
                    return DUPLICATE;
                }
                // If the root event was created before the previous exec time for the same
                // leaf event - we can deduplicate cause by the time of the prev execution
                // the template (root_event source) changes were already in the database.
                if (previousExecutionTime
                        && message.root_event
                        && new Date(previousExecutionTime) - 1000 >
                            new Date(message.root_event.dt)) {
                    hyper.metrics.increment(`${name}_dedupe`);
                    hyper.logger.log('trace/dedupe', () => ({
                        message: 'Event was deduplicated based on sha1 and root_event dt',
                        event_str: utils.stringify(message),
                        newer_dt: previousExecutionTime
                    }));
                    return DUPLICATE;
                }
                return this._redis.setAsync(messageKey, new Date().toISOString())
                .then(() => this._redis.expireAsync(messageKey,
                    Math.ceil(this._expire_timeout / 24)))
                .thenReturn(NOT_DUPLICATE);
            });
        })
        .then((sha1Duplicate) => {
            if (sha1Duplicate.body || !message.root_event) {
                // If the message was sha1-deduped or if it has no root event info,
                // don't use deduplication by the root event
                return sha1Duplicate;
            }

            const rootEventKey = `${this._prefix}_dedupe_${name}_${message.root_event.signature}`;
            return this._redis.getAsync(rootEventKey)
            .then((oldEventTimestamp) => {
                // If this event was caused by root event and there was a leaf event executed
                // already that belonged to a later root_event we can cut off this chain.
                if (oldEventTimestamp
                        && new Date(oldEventTimestamp) > new Date(message.root_event.dt)) {
                    hyper.metrics.increment(`${name}_dedupe`);
                    hyper.logger.log('trace/dedupe', () => ({
                        message: 'Event was deduplicated based on root event',
                        event_str: utils.stringify(message),
                        signature: message.root_event.signature,
                        newer_dt: oldEventTimestamp
                    }));
                    return DUPLICATE;
                }
                return this._redis.setAsync(rootEventKey, message.root_event.dt)
                .then(() => this._redis.expireAsync(rootEventKey, this._expire_timeout))
                .thenReturn(NOT_DUPLICATE);
            });
        })
        .catch((e) => {
            hyper.logger.log('error/dedupe', {
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
