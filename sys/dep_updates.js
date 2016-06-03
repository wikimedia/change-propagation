"use strict";

const extend = require('extend');
const HyperSwitch = require('hyperswitch');
const Template = HyperSwitch.Template;
const utils = require('../lib/utils');
const Title = require('mediawiki-title').Title;

const BACKLINKS_CONTINUE_TOPIC_NAME = 'change-prop.backlinks.continue';
const TRANSCLUDES_CONTINUE_TOPIC_NAME = 'change-prop.transcludes.continue';

function createBackLinksTemplate(options) {
    return {
        template: new Template(extend(true, {}, options.templates.mw_api, {
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
        })),
        getContinueToken: (res) => res.body.continue && res.body.continue.blcontinue,
        continueTopic: BACKLINKS_CONTINUE_TOPIC_NAME,
        resourceChangeTag: 'backlinks',
        extractResults: (res) => res.body.query.backlinks
    };
}

function createImageUsageTemplate(options) {
    return {
        template: new Template(extend(true, {}, options.templates.mw_api, {
            method: 'post',
            body: {
                format: 'json',
                action: 'query',
                list: 'imageusage',
                iutitle: '{{request.params.title}}',
                iucontinue: '{{message.continue}}',
                iulimit: 500
                // TODO: decide what do we want to do with redirects
            }
        })),
        getContinueToken: (res) => res.body.continue && res.body.continue.iucontinue,
        continueTopic: TRANSCLUDES_CONTINUE_TOPIC_NAME,
        resourceChangeTag: 'transcludes',
        extractResults: (res) =>  res.body.query.imageusage
    };
}

function createTranscludeInTemplate(options) {
    return {
        template: new Template(extend(true, {}, options.templates.mw_api, {
            method: 'post',
            body: {
                format: 'json',
                action: 'query',
                prop: 'transcludedin',
                tiprop: 'title',
                tishow: '!redirect',
                titles: '{{request.params.title}}',
                ticontinue: '{{message.continue}}',
                tilimit: 500
            }
        })),
        getContinueToken: (res) => res.body.continue && res.body.continue.ticontinue,
        continueTopic: TRANSCLUDES_CONTINUE_TOPIC_NAME,
        resourceChangeTag: 'transcludes',
        extractResults: (res) => {
            return res.body.query.pages[Object.keys(res.body.query.pages)[0]].transcludedin;
        }
    };
}

class DependencyProcessor {
    constructor(options) {
        this.options = options;
        this.siteInfoCache = {};
        this.backLinksRequest = createBackLinksTemplate(options);
        this.imageLinksRequest = createImageUsageTemplate(options);
        this.transcludeInRequest = createTranscludeInTemplate(options);
        this.siteInfoRequest = new Template(extend(true, {}, options.templates.mw_api, {
            method: 'post',
            body: {
                format: 'json',
                action: 'query',
                meta: 'siteinfo',
                siprop: 'general|namespaces|namespacealiases'
            }
        }));
    }

    setup(hyper) {
        return hyper.post({
            uri: '/sys/queue/subscriptions',
            body: {
                backlinks_continue: {
                    topic: BACKLINKS_CONTINUE_TOPIC_NAME,
                    exec: [
                        {
                            method: 'post',
                            uri: '/sys/links/backlinks/{message.original_event.title}',
                            body: '{{globals.message}}'
                        }
                    ]
                },
                transclusions_continue: {
                    topic: TRANSCLUDES_CONTINUE_TOPIC_NAME,
                    exec: [
                        {
                            method: 'post',
                            uri: '/sys/links/transcludes/{message.original_event.page_title}',
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
        const originalEvent = req.body.original_event || req.body;
        return this._getSiteInfo(hyper, req.body)
        .then((siteInfo) => {
            return this._fetchAndProcessBatch(hyper, this.backLinksRequest,
                context, siteInfo, originalEvent);
        });
    }

    processTranscludes(hyper, req) {
        const message = req.body;
        const context = {
            request: req,
            message: message
        };
        const originalEvent = req.body.original_event || req.body;
        return this._getSiteInfo(hyper, message)
        .then((siteInfo) => {
            const title = Title.newFromText(req.params.title, siteInfo);
            // First step - process only file uploads
            if (title.getNamespace().isFile()) {
                return this._fetchAndProcessBatch(hyper, this.imageLinksRequest,
                    context, siteInfo, originalEvent);
            }
            return { status: 200 };
        });
    }

    _fetchAndProcessBatch(hyper, requestTemplate, context, siteInfo, originalEvent) {
        return hyper.post(requestTemplate.template.expand(context))
        .then((res) => {
            const titles = requestTemplate.extractResults(res).map((item) => {
                return {
                    title: Title.newFromText(item.title, siteInfo).getPrefixedDBKey()
                };
            });
            let actions = this._sendResourceChanges(hyper, titles,
                originalEvent, requestTemplate.resourceChangeTag);
            if (res.body.continue) {
                actions = actions.then(() =>
                    this._sendContinueEvent(hyper,
                        requestTemplate.continueTopic,
                        originalEvent,
                        requestTemplate.getContinueToken(res)));
            }
            return actions.thenReturn({ status: 200 });
        });
    }

    _sendContinueEvent(hyper, topic, origEvent, continueToken) {
        return hyper.post({
            uri: '/sys/queue/events',
            body: [{
                meta: {
                    topic: topic,
                    schema_uri: 'continue/1',
                    uri: origEvent.meta.uri,
                    request_id: origEvent.meta.request_id,
                    domain: origEvent.meta.domain,
                    dt: origEvent.meta.dt
                },
                triggered_by: utils.triggeredBy(origEvent),
                original_event: origEvent,
                continue: continueToken
            }]
        });
    }

    _sendResourceChanges(hyper, items, originalEvent, tag) {
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
                        dt: originalEvent.meta.dt
                    },
                    triggered_by: utils.triggeredBy(originalEvent),
                    tags: [ 'change-prop', tag ]
                };
            })
        });
    }

    _getSiteInfo(hyper, message) {
        const domain = message.meta.domain;
        if (!this.siteInfoCache[domain]) {
            this.siteInfoCache[domain] = hyper.post(this.siteInfoRequest.expand({
                message: message
            }))
            .then((res) => {
                return {
                    general: {
                        lang: res.body.query.general.lang,
                        legaltitlechars: res.body.query.general.legaltitlechars,
                        case: res.body.query.general.case
                    },
                    namespaces: res.body.query.namespaces,
                    namespacealiases: res.body.query.namespacealiases
                };
            });
        }
        return this.siteInfoCache[domain];
    }
}

module.exports = (options) => {
    const processor = new DependencyProcessor(options);
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
                },
                '/transcludes/{title}': {
                    post: {
                        summary: 'check if the page is transcluded somewhere and update',
                        operationId: 'process_transcludes'
                    }
                }
            }
        },
        operations: {
            process_backlinks: processor.processBackLinks.bind(processor),
            process_transcludes: processor.processTranscludes.bind(processor),
            setup: processor.setup.bind(processor)
        },
        resources: [{
            uri: '/sys/links/setup'
        }]
    };
};
