"use strict";

const utils = require('../lib/utils');
const HTTPError = require('hyperswitch').HTTPError;

class OresUpdater {
    constructor(options) {
        this.options = options;
        if (!options.uri) {
            throw new Error('Invalid options of ORES module, uri required');
        }
    }

    updateORES(hyper, req) {
        const wiki = req.body.database;
        const rev_id = req.body.rev_id;
        if (!this.options.models[wiki]) {
            throw new HTTPError({
                status: 400,
                body: {
                    message: `ORES precache request for unmodified wiki ${wiki}`
                }
            });
        }

        return hyper.get(utils.augmentRequest({
            uri: this.options.uri + '/v2/scores/' + wiki + '/',
            query: {
                models: this.options.models[wiki],
                revids: rev_id,
                precache: true
            }
        }, req.body));
    }
}

module.exports = function(options) {
    const oresUpdater = new OresUpdater(options);
    return {
        spec: {
            paths: {
                '/': {
                    post: {
                        operationId: 'updateORES'
                    }
                }
            }
        },
        operations: {
            updateORES: oresUpdater.updateORES.bind(oresUpdater)
        }
    };
};