'use strict';

const Purger = require('htcp-purge');
const HyperSwitch = require('hyperswitch');
const HTTPError = HyperSwitch.HTTPError;
const lifecicle = HyperSwitch.lifecycle;
const utils = require('../lib/utils');

class PurgeService {
    constructor(options) {
        this.options = options || {};
        if (!this.options.host || !this.options.port) {
            throw new Error('Purging module must be configured with host and port');
        }
        this.purger = new Purger({
            log: this.options.log,
            routes: [ this.options ]
        });
        lifecicle.on('close', () => this.purger.close());
    }

    init() {
        return this.purger.bind().thenReturn({ status: 201 });
    }

    purge(hyper, req) {
        if (!Array.isArray(req.body)) {
            throw new HTTPError({
                status: 400,
                body: {
                    type: 'bad_request',
                    description: 'Invalid request for purge service.'
                }
            });
        }

        return this.purger.purge(req.body.map((event) => {
            if (!event.meta || !event.meta.uri || !/^(https?:)?\/\//.test(event.meta.uri)) {
                hyper.logger.log('error/events/purge', () => ({
                    message: 'Invalid event URI',
                    event_str: utils.stringify(event)
                }));
                return undefined;
            }
            return `http:${event.meta.uri.replace(/^https?:/, '')}`;
        }).filter((event) => !!event))
            .thenReturn({ status: 201 })
            .catch((e) => {
                throw new HTTPError({
                    status: 500,
                    details: e.message
                });
            });
    }
}

module.exports = (options) => {
    const ps = new PurgeService(options);

    return {
        spec: {
            paths: {
                '/': {
                    post: {
                        operationId: 'purge'
                    }
                },
                '/init': {
                    put: {
                        operationId: 'init'
                    }
                }
            }
        },
        operations: {
            init: ps.init.bind(ps),
            purge: ps.purge.bind(ps)
        },
        resources: [ {
            uri: '/sys/purge/init'
        } ]
    };
};
