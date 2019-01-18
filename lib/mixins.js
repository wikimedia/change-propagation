'use strict';

const P = require('bluebird');
const redis = require('redis');
const HyperSwitch = require('hyperswitch');

const Redis = (superclass) => class extends superclass {
    constructor(options) {
        if (superclass !== Object) {
            super(options);
        } else {
            // Don't need to pass the options to the Object super constructor.
            super();
        }

        if (!options.redis) {
            throw new Error('Redis options not provided to the rate_limiter');
        }

        if (!(options.redis.host && options.redis.port) && !options.redis.path) {
            throw new Error('Redis host:port or unix socket path must be specified');
        }

        options.redis = Object.assign(options.redis, {
            no_ready_check: true // Prevents sending unsupported info command to nutcracker
        });
        this._redis = P.promisifyAll(redis.createClient(options.redis));
        this._redis.on('error', (e) => {
            // If we can't connect to redis - don't worry and don't fail,
            // just log it and ignore.
            options.logger.log('error/redis', e);
        });
        HyperSwitch.lifecycle.on('close', () => this._redis.quit());
    }
};

class MixinBuilder {
    constructor(superclass) {
        this.superclass = superclass;
    }

    with() {
        return Array.prototype.slice.call(arguments)
            .reduce((c, mixin) => mixin(c), this.superclass);
    }
}

module.exports = {
    mix: (superclass) => new MixinBuilder(superclass),
    Redis
};
