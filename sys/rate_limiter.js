'use strict';

const Limiter = require('ratelimit.js').RateLimit;
const P = require('bluebird');
const HyperSwitch = require('hyperswitch');
const HTTPError = HyperSwitch.HTTPError;
const mixins = require('../lib/mixins');

class RateLimiter extends mixins.mix(Object).with(mixins.Redis) {
    constructor(options) {
        super(options);

        this._options = options;

        this._LIMITERS = new Map();
        Object.keys(this._options.limiters).forEach((type) => {
            let limiterOpts = this._options.limiters[ type ];

            if (!Array.isArray(limiterOpts)) {
                limiterOpts = [ limiterOpts ];
            }

            limiterOpts.forEach((opt) => {
                if (!opt.interval || !opt.limit) {
                    throw new Error(`Limiter ${type} is miconfigured`);
                }
            });

            this._LIMITERS.set(type, new Limiter(this._redis, limiterOpts,
                { prefix: `CPLimiter_${type}` }));
        });
    }

    _execLimiterFun(fun, hyper, type, key) {
        const limiter = this._LIMITERS.get(type);

        if (!limiter) {
            hyper.logger.log('warn/ratelimit', {
                msg: 'Unconfigured rate-limiter is used',
                limiter_type: type
            });
            return { status: 204 };
        }

        const startTime = Date.now();

        return new P((resolve, reject) => {
            limiter[ fun ](key, (err, isRateLimited) => {
                if (err) {
                    hyper.logger.log('error/ratelimit', err);
                    hyper.metrics.endTiming(`ratelimit.${fun}.err`, startTime);
                    // In case we've got problems with limiting just allow everything
                    return resolve({ status: 200 });
                }

                if (isRateLimited) {
                    hyper.metrics.endTiming(`ratelimit.${fun}.block`, startTime);
                    return reject(new HTTPError({
                        status: 429,
                        body: {
                            type: 'rate_limit',
                            message: `Message rejected by limiter ${type}`,
                            key
                        }
                    }));
                }

                hyper.metrics.endTiming(`ratelimit.${fun}.allow`, startTime);
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
                        operationId: 'check',
                        // XXX: Ugly hack below so that the automatic monitoring
                        // script does not complain about it correctly returning 403
                        'x-monitor': false
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
