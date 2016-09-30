"use strict";

const Purger    = require('htcp-purge');
const HTTPError = require('hyperswitch').HTTPError;

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
            if (!event.meta || !event.meta.uri || !/^\/\//.test(event.meta.uri)) {
                hyper.log('error/events/purge', {
                    message: 'Invalid event URI',
                    event
                });
                return undefined;
            }
            return `http:${event.meta.uri}`;
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
                }
            }
        },
        operations: {
            purge: ps.purge.bind(ps)
        }
    };
};
