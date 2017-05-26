"use strict";

const Limiter = require('ratelimit.js').RateLimit;
const redis = require('redis');
const P = require('bluebird');
const HyperSwitch = require('hyperswitch');
const HTTPError = HyperSwitch.HTTPError;

class RateLimiter {
    constructor(options) {
        this._options = options;
        this._log = this._options.log || (() => {});

        // TODO: verify and pass in the redis options
        this._client = redis.createClient(this._options.redis);
        HyperSwitch.lifecycle.on('close', () => this._client.quit());

        // TODO: configurable options

        this._LIMITERS = new Map();
        Object.keys(this._options.limiters).forEach((type) => {
            const limiterOpts = this._options.limiters[type];
            this._LIMITERS.set(type, new Limiter(this._client, [
                limiterOpts
            ], { prefix: `CPLimiter_${type}` }));
        });
    }

    _execLimiterFun(fun, hyper, type, key) {
        const limiter = this._LIMITERS.get(type);
        if (!limiter) {
            return { status: 204 };
        }

        return new P((resolve, reject) => {
            limiter[fun](key, (err, isRateLimited) => {
                if (err) {
                    // TODO: log!
                    // In case we've got problems with limiting just allow everything
                    return resolve({ status: 200 });
                }

                if (isRateLimited) {
                    return reject(new HTTPError({
                        status: 429,
                        body: {
                            type: 'rate_limit',
                            message: `Message rejected by limiter ${type}`,
                            key
                        }
                    }));
                }
                return resolve({ status: 201 });
            });
        });
    }

    increment(hyper, req) {
        return this._execLimiterFun('incr', hyper, req.params.type, req.params.key);
    }

    check(hyper, req) {
        return this._execLimiterFun('check', hyper, req.params.type, req.params.key);
    }
}

module.exports = (options) => {
    const ps = new RateLimiter(options);

    return {
        spec: {
            paths: {
                '/{type}/{key}': {
                    post: {
                        operationId: 'incrementAndCheck'
                    },
                    get: {
                        operationId: 'check'
                    }
                }
            }
        },
        operations: {
            incrementAndCheck: ps.increment.bind(ps),
            check: ps.check.bind(ps)
        }
    };
};
