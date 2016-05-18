"use strict";

const extend = require('extend');
const HyperSwitch = require('hyperswitch');
const Template = HyperSwitch.Template;
const utils = require('../lib/utils');

const CONTINUE_TOPIC_NAME = 'change-prop.backlinks.continue';

class BackLinksProcessor {
    constructor(options) {
        this.options = options;
        this.backLinksRequest = new Template(extend(true, {}, options.templates.mw_api, {
            method: 'post',
            body: {
                format: 'json',
                action: 'query',
                list: 'backlinks',
                bltitle: '{{request.params.title}}',
                blfilterredir: 'nonredirects',
                blcontinue: '{{message.continue}}',
                bllimit: 500
            }
        }));
    }

    setup(hyper) {
        return hyper.post({
            uri: '/sys/queue/subscriptions',
            body: {
                backlinks_continue: {
                    topic: CONTINUE_TOPIC_NAME,
                    exec: [
                        {
                            method: 'post',
                            uri: '/sys/links/backlinks/{message.original_event.title}',
                            body: '{{globals.message}}'
                        }
                    ]
                }
            }
        });
    }

    processBackLinks(hyper, req) {
        const context = {
            request: req,
            message: req.body
        };
        return hyper.post(this.backLinksRequest.expand(context))
        .then((res) => {
            const originalEvent = req.body.original_event || req.body;
            let actions = this._sendResourceChanges(hyper, res.body.query.backlinks, originalEvent);
            if (res.body.continue) {
                actions = actions.then(() => hyper.post({
                    uri: '/sys/queue/events',
                    body: [{
                        meta: {
                            topic: CONTINUE_TOPIC_NAME,
                            schema_uri: 'continue/1',
                            uri: originalEvent.meta.uri,
                            request_id: originalEvent.meta.request_id,
                            domain: originalEvent.meta.domain,
                            dt: originalEvent.meta.dt,
                            triggered_by: utils.triggeredBy(originalEvent)
                        },
                        original_event: originalEvent,
                        continue: res.body.continue.blcontinue
                    }]
                }));
            }
            return actions.thenReturn({ status: 200 });
        });
    }

    _sendResourceChanges(hyper, items, originalEvent) {
        return hyper.post({
            uri: '/sys/queue/events',
            body: items.map((item) => {
                return {
                    meta: {
                        topic: 'resource_change',
                        schema_uri: 'resource_change/1',
                        // TODO: need to check whether a wiki is http or https!
                        uri: `https://${originalEvent.meta.domain}/wiki/${item.title}`,
                        request_id: originalEvent.meta.request_id,
                        domain: originalEvent.meta.domain,
                        dt: originalEvent.meta.dt,
                        triggered_by: utils.triggeredBy(originalEvent)
                    },
                    tags: [ 'change-prop', 'backlinks' ]
                };
            })
        });
    }
}

module.exports = (options) => {
    const processor = new BackLinksProcessor(options);
    return {
        spec: {
            paths: {
                '/setup': {
                    put: {
                        summary: 'setup the module',
                        operationId: 'setup'
                    }
                },
                '/backlinks/{title}': {
                    post: {
                        summary: 'set up the kafka listener',
                        operationId: 'process_backlinks'
                    }
                }
            }
        },
        operations: {
            process_backlinks: processor.processBackLinks.bind(processor),
            setup: processor.setup.bind(processor)
        },
        resources: [{
            uri: '/sys/links/setup'
        }]
    };
};
