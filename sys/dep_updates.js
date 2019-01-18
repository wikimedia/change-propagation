'use strict';

const extend = require('extend');
const HyperSwitch = require('hyperswitch');
const Template = HyperSwitch.Template;
const utils = require('../lib/utils');
const Title = require('mediawiki-title').Title;

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
        resourceChangeTags: [ 'backlinks' ],
        leafTopicName: 'change-prop.backlinks.resource-change',
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
        leafTopicName: 'change-prop.transcludes.resource-change',
        resourceChangeTags: [ 'transcludes', 'files' ],
        extractResults: (res) => res.body.query.imageusage
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
        leafTopicName: 'change-prop.transcludes.resource-change',
        resourceChangeTags: [ 'transcludes', 'templates' ],
        extractResults: (res) => {
            return res.body.query.pages[ Object.keys(res.body.query.pages)[ 0 ] ].transcludedin;
        }
    };
}

function createWikidataTemplate(options) {
    return {
        template: new Template(extend(true, {}, options.templates.mw_api, {
            method: 'post',
            body: {
                format: 'json',
                action: 'wbgetentities',
                ids: '{{message.page_title}}',
                props: 'sitelinks/urls',
                normalize: true
            }
        })),
        resourceChangeTags: [ 'wikidata' ],
        leafTopicName: 'change-prop.wikidata.resource-change',
        shouldProcess: (res) => {
            if (!(res && res.body && !!res.body.success)) {
                return false;
            }
            const entities = res.body.entities || {};
            if (!Object.keys(entities).length) {
                return false;
            }
            const sitelinks = entities[ Object.keys(entities)[ 0 ] ].sitelinks || {};
            if (!Object.keys(sitelinks).length) {
                return false;
            }
            return true;
        },
        extractResults: (res) => {
            const siteLinks = res.body.entities[ Object.keys(res.body.entities)[ 0 ] ].sitelinks;
            return Object.keys(siteLinks).map((siteId) => {
                const pageURI = siteLinks[ siteId ].url;
                const match = /^https?:\/\/([^/]+)\/wiki\/(.+)$/.exec(pageURI);
                return {
                    domain: match[ 1 ],
                    title: decodeURIComponent(match[ 2 ])
                };
            });
        }
    };
}

function _sendContinueEvent(hyper, topic, origEvent, continueToken) {
    return hyper.post({
        uri: '/sys/queue/events',
        body: [ {
            meta: {
                topic,
                schema_uri: 'continue/1',
                uri: origEvent.meta.uri,
                request_id: origEvent.meta.request_id,
                domain: origEvent.meta.domain,
                dt: new Date().toISOString()
            },
            triggered_by: utils.triggeredBy(origEvent),
            root_event: {
                signature: origEvent.meta.uri,
                dt: origEvent.meta.dt
            },
            original_event: origEvent,
            continue: continueToken
        } ]
    });
}

function _sendResourceChanges(hyper, items, originalEvent, tags, topicName) {
    return hyper.post({
        uri: '/sys/queue/events',
        body: items.map((item) => {
            // TODO: need to check whether a wiki is http or https!
            const resourceURI =
                `https://${item.domain}/wiki/${encodeURIComponent(item.title)}`;
            return {
                meta: {
                    topic: topicName,
                    schema_uri: 'resource_change/1',
                    uri: resourceURI,
                    request_id: originalEvent.meta.request_id,
                    domain: item.domain,
                    dt: new Date().toISOString()
                },
                triggered_by: utils.triggeredBy(originalEvent),
                tags,
                root_event: {
                    signature: originalEvent.meta.uri,
                    dt: originalEvent.meta.dt
                }
            };
        })
    });
}

class DependencyProcessor {
    constructor(options) {
        this.options = options;
        this.siteInfoCache = {};
        this.backLinksRequest = createBackLinksTemplate(options);
        this.imageLinksRequest = createImageUsageTemplate(options);
        this.transcludeInRequest = createTranscludeInTemplate(options);
        this.wikidataRequest = createWikidataTemplate(options);
        this.siteInfoRequest = new Template(extend(true, {}, options.templates.mw_api, {
            method: 'post',
            body: {
                format: 'json',
                action: 'query',
                meta: 'siteinfo',
                siprop: 'general|namespaces|namespacealiases|specialpagealiases'
            }
        }));
        this.latestMessages = [];
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
            message
        };
        const originalEvent = req.body.original_event || req.body;
        return this._getSiteInfo(hyper, message)
            .then((siteInfo) => {
                const title = Title.newFromText(req.params.title, siteInfo);
                if (title.getNamespace().isFile()) {
                    return this._fetchAndProcessBatch(hyper, this.imageLinksRequest,
                        context, siteInfo, originalEvent);
                }
                return this._fetchAndProcessBatch(hyper, this.transcludeInRequest,
                    context, siteInfo, originalEvent);
            });
    }

    processWikidata(hyper, req) {
        const context = {
            request: req,
            message: req.body
        };
        return hyper.request(this.wikidataRequest.template.expand(context))
            .then((res) => {
                if (this.wikidataRequest.shouldProcess(res)) {
                    const items = this.wikidataRequest.extractResults(res);
                    return _sendResourceChanges(hyper, items, req.body,
                        this.wikidataRequest.resourceChangeTags,
                        this.wikidataRequest.leafTopicName);
                }

                if (res.body && res.body.error) {
                    hyper.logger.log('warn/wikidata_description', () => ({
                        msg: 'Could not extract items',
                        event_str: utils.stringify(context.message),
                        error: res.body.error
                    }));
                }
            })
            .thenReturn({ status: 200 });
    }

    _fetchAndProcessBatch(hyper, requestTemplate, context, siteInfo, originalEvent) {
        return hyper.post(requestTemplate.template.expand(context))
            .then((res) => {
                const titles = (requestTemplate.extractResults(res) || []).map((item) => {
                    return {
                        title: Title.newFromText(item.title, siteInfo).getPrefixedDBKey(),
                        domain: originalEvent.meta.domain
                    };
                });
                if (!titles.length) {
                // the batch is complete or the list of transcluded titles is empty
                    return { status: 200 };
                }
                let actions = _sendResourceChanges(hyper, titles,
                    originalEvent, requestTemplate.resourceChangeTags,
                    requestTemplate.leafTopicName);
                if (res.body.continue) {
                    actions = actions.then(() => _sendContinueEvent(hyper,
                        requestTemplate.leafTopicName,
                        originalEvent,
                        requestTemplate.getContinueToken(res)));
                }
                return actions.thenReturn({ status: 200 });
            });
    }

    _getSiteInfo(hyper, message) {
        const domain = message.meta.domain;
        if (!this.siteInfoCache[ domain ]) {
            this.siteInfoCache[ domain ] = hyper.post(this.siteInfoRequest.expand({
                message
            }))
                .then((res) => {
                    if (!res || !res.body || !res.body.query || !res.body.query.general) {
                        throw new Error(`SiteInfo is unavailable for ${message.meta.domain}`);
                    }
                    return {
                        general: {
                            lang: res.body.query.general.lang,
                            legaltitlechars: res.body.query.general.legaltitlechars,
                            case: res.body.query.general.case
                        },
                        namespaces: res.body.query.namespaces,
                        namespacealiases: res.body.query.namespacealiases,
                        specialpagealiases: res.body.query.specialpagealiases
                    };
                })
                .catch((e) => {
                    hyper.logger.log('error/site_info', e);
                    delete this.siteInfoCache[ domain ];
                    throw e;
                });
        }
        return this.siteInfoCache[ domain ];
    }
}

module.exports = (options) => {
    const processor = new DependencyProcessor(options);
    return {
        spec: {
            paths: {
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
                },
                '/wikidata_descriptions': {
                    post: {
                        summary: 'find all pages corresponding to ' +
                            'the wikidata item and update summary',
                        operationId: 'process_wikidata'
                    }
                }
            }
        },
        operations: {
            process_backlinks: processor.processBackLinks.bind(processor),
            process_transcludes: processor.processTranscludes.bind(processor),
            process_wikidata: processor.processWikidata.bind(processor)
        }
    };
};
