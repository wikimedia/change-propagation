"use strict";

const P = require('bluebird');
const uuid = require('cassandra-uuid').TimeUuid;

class OresProcessor {
    constructor(options) {
        this._options = options;
        if (!options.ores_precache_uris || !Array.isArray(options.ores_precache_uris)) {
            throw new Error('OresProcessor is miconfigured. ores_precache_uris is required');
        }
        if (!options.eventbus_uri) {
            throw new Error('OresProcessor is miconfigured. eventbus_uri is required');
        }
    }

    process(hyper, req) {
        const message = req.body;
        return P.all(this._options.ores_precache_uris.map(uri => hyper.post({
            uri,
            headers: {
                'content-type': 'application/json'
            },
            body: message
        })))
        .spread((res) => {
            if (!req.query.postevent || !res.body) {
                return res;
            }
            const now = new Date();
            const newMessage = {
                meta: {
                    topic: 'mediawiki.revision-score',
                    uri: message.meta.uri,
                    request_id: message.meta.request_id,
                    id: uuid.fromDate(now).toString(),
                    dt: now.toISOString(),
                    domain: message.meta.domain,
                },
                database: message.database,
                page_id: message.page_id,
                page_title: message.page_title,
                page_namespace: message.page_namespace,
                performer: message.performer,
                rev_id: message.rev_id,
                rev_parent_id: message.rev_parent_id,
                rev_timestamp: message.rev_timestamp
            };
            // newMessage.scores and/or newMessage.errors
            // will be provided only if there are any
            // elements for those arrays.
            // https://phabricator.wikimedia.org/T210465#4797591


            const domainScores = res.body[newMessage.database];
            const revScores = domainScores.scores[`${newMessage.rev_id}`];
            Object.keys(revScores).forEach((modelName) => {
                // The example schema for the score object:
                // {
                //   "model_name": "awesomeness",
                //   "model_version": "1.0",
                //   "prediction": ["yes", "mostly"],
                //   "probability": [
                //     {"name": "yes", "value": 0.99},
                //     {"name": "mostly", "value": 0.90},
                //     {"name": "hardly", "value": 0.01}
                //   ]
                // }
                const score = {
                    model_name: modelName,
                    model_version: domainScores.models[modelName].version,
                };
                if (revScores[modelName].error) {
                    Object.assign(score, revScores[modelName].error);
                    newMessage.errors = newMessage.errors || [];
                    newMessage.errors.push(score);
                } else {
                    const originalScore = revScores[modelName].score;
                    // Convert prediction to array of strings
                    let prediction = originalScore.prediction;
                    if (!Array.isArray(prediction)) {
                        prediction = [ prediction ];
                    }
                    score.prediction = prediction.map(p => `${p}`);
                    // Convert probabilities to an array of name-value pairs.
                    score.probability = Object.keys(originalScore.probability).map(probName => ({
                        name: probName,
                        value: originalScore.probability[probName]
                    }));
                    newMessage.scores = newMessage.scores || [];
                    newMessage.scores.push(score);
                }
            });
            return hyper.post({
                uri: this._options.eventbus_uri,
                headers: {
                    'content-type': 'application/json'
                },
                body: [ newMessage ]
            });
        });
    }
}

module.exports = (options) => {
    const processor = new OresProcessor(options);
    return {
        spec: {
            paths: {
                '/': {
                    post: {
                        summary: 'submit revision-create for processing',
                        operationId: 'process'
                    }
                }
            }
        },
        operations: {
            process: processor.process.bind(processor),
        }
    };
};
