'use strict';

const ChangeProp = require('../utils/changeProp');
const nock       = require('nock');
const uuid       = require('cassandra-uuid').TimeUuid;
const common     = require('../utils/common');
const dgram      = require('dgram');
const assert     = require('assert');
const P          = require('bluebird');
const preq       = require('preq');
const Ajv        = require('ajv');

process.env.UV_THREADPOOL_SIZE = 128;

describe('update rules', function() {
    this.timeout(30000);

    const changeProp = new ChangeProp('config.example.wikimedia.yaml');
    let producer;
    let siteInfoResponse;

    before(function() {
        // Setting up might take some tome, so disable the timeout
        this.timeout(50000);
        return changeProp.start()
        .then(() => {
            return preq.post({
                uri: 'https://en.wikipedia.org/w/api.php',
                body: {
                    formatversion: '2',
                    format: 'json',
                    action: 'query',
                    meta: 'siteinfo',
                    siprop: 'general|namespaces|namespacealiases|specialpagealiases'
                }
            });
        })
        .then((res) => siteInfoResponse = res.body)
        .then(() => common.factory.createProducer({ log: console.log.bind(console) }))
        .then((result) => producer = result);
    });

    const nockWithOptionalSiteInfo = () => nock('https://en.wikipedia.org')
        .post('/w/api.php', {
            formatversion: '2',
            format: "json",
            action: "query",
            meta: "siteinfo",
            siprop: "general|namespaces|namespacealiases|specialpagealiases"
        })
        .optionally()
        .reply(200, siteInfoResponse);

    function summaryEndpointTest(topic) {
        const mwAPI = nock('https://en.wikipedia.org', {
            reqheaders: {
                'cache-control': 'no-cache',
                'x-triggered-by': `req:${common.SAMPLE_REQUEST_ID},${topic}:https://en.wikipedia.org/api/rest_v1/page/html/Main_Page`,
                'user-agent': 'SampleChangePropInstance'
            }
        })
        .get('/api/rest_v1/page/summary/Main_Page')
        .query({ redirect: false })
        .reply(200, { });

        return P.try(() => producer.produce(`test_dc.${topic}`, 0,
            Buffer.from(JSON.stringify({
                meta: {
                    topic: topic,
                    schema_uri: 'resource_change/1',
                    uri: 'https://en.wikipedia.org/api/rest_v1/page/html/Main_Page',
                    request_id: common.SAMPLE_REQUEST_ID,
                    id: uuid.now(),
                    dt: new Date().toISOString(),
                    domain: 'en.wikipedia.org'
                },
                tags: ['restbase']
            }))))
        .then(() => common.checkAPIDone(mwAPI))
        .finally(() => nock.cleanAll());
    }

    function testPurgeCacheOnResourceChange(uriBefore, uriAfter, domain, tags, testString, done) {
        var udpServer = dgram.createSocket('udp4');
        let closed = false;
        udpServer.on("message", function(msg) {
            try {
                msg = msg.slice(22, 22 + msg.readInt16BE(20)).toString();
                if (msg.indexOf(testString) >= 0) {
                    assert.deepEqual(msg, uriAfter)
                    udpServer.close();
                    closed = true;
                    done();
                }
            } catch (e) {
                udpServer.close();
                closed = true;
                done(e);
            }
        });
        udpServer.bind(4321);

        P.try(() => producer.produce('test_dc.resource_change', 0,
            Buffer.from(JSON.stringify({
                meta: {
                    topic: 'resource_change',
                    schema_uri: 'resource_change/1',
                    uri: uriBefore,
                    request_id: uuid.now(),
                    id: uuid.now(),
                    dt: new Date().toISOString(),
                    domain
                },
                tags
            }))))
        .delay(common.REQUEST_CHECK_DELAY)
        .finally(() => {
            if (!closed) {
                udpServer.close();
                done(new Error('Timeout!'));
            }
        });
    }



    it('Should update summary endpoint', () =>
        summaryEndpointTest('resource_change'));

    it('Should update summary endpoint, transcludes topic', () =>
        summaryEndpointTest('change-prop.transcludes.resource-change'));

    it('Should update summary endpoint on page images change', () => {
        const mwAPI = nock('https://en.wikipedia.org', {
            reqheaders: {
                'cache-control': 'no-cache',
                'x-triggered-by': `req:${common.SAMPLE_REQUEST_ID},mediawiki.page-properties-change:https://en.wikipedia.org/wiki/Some_Page`,
                'user-agent': 'SampleChangePropInstance'
            }
        })
        .get('/api/rest_v1/page/summary/Some_Page')
        .query({ redirect: false })
        .reply(200, { });

        return P.try(() => producer.produce('test_dc.mediawiki.page-properties-change', 0,
            Buffer.from(JSON.stringify({
                meta: {
                    topic: 'mediawiki.page-properties-change',
                    schema_uri: 'mediawiki/page/properties-change/1',
                    uri: 'https://en.wikipedia.org/wiki/Some_Page',
                    request_id: common.SAMPLE_REQUEST_ID,
                    id: uuid.now(),
                    dt: new Date().toISOString(),
                    domain: 'en.wikipedia.org'
                },
                added_properties: {
                    page_image: 'Test.jpg'
                },
                page_title: 'Some_Page'
            }))))
        .then(() => common.checkAPIDone(mwAPI))
        .finally(() => nock.cleanAll());
    });

    it('Should not update summary for a blacklisted title', () => {
        const mwAPI = nock('https://en.wikipedia.org', {
            reqheaders: {
                'cache-control': 'no-cache',
                'x-triggered-by': `req:${common.SAMPLE_REQUEST_ID},resource_change:https://en.wikipedia.org/api/rest_v1/page/html/User%3ACyberbot_I%2FTest`,
                'user-agent': 'SampleChangePropInstance'
            }
        })
        .get('/api/rest_v1/page/summary/User%3ACyberbot_I%2FTest')
        .query({ redirect: false })
        .reply(200, { });

        return P.try(() => producer.produce('test_dc.resource_change', 0,
            Buffer.from(JSON.stringify({
                meta: {
                    topic: 'resource_change',
                    schema_uri: 'resource_change/1',
                    uri: 'https://en.wikipedia.org/api/rest_v1/page/html/User%3ACyberbot_I%2FTest',
                    request_id: common.SAMPLE_REQUEST_ID,
                    id: uuid.now(),
                    dt: new Date().toISOString(),
                    domain: 'en.wikipedia.org'
                },
                tags: ['restbase']
            }))))
        .then(() => common.checkPendingMocks(mwAPI, 1))
        .finally(() => nock.cleanAll());
    });

    it('Should update definition endpoint', () => {
        const mwAPI = nock('https://en.wiktionary.org', {
            reqheaders: {
                'cache-control': 'no-cache',
                'x-triggered-by': `req:${common.SAMPLE_REQUEST_ID},resource_change:https://en.wiktionary.org/api/rest_v1/page/html/Main_Page`,
                'user-agent': 'SampleChangePropInstance'
            }
        })
        .get('/api/rest_v1/page/definition/Main_Page')
        .query({ redirect: false })
        .reply(200, {});

        return P.try(() => producer.produce('test_dc.resource_change', 0,
            Buffer.from(JSON.stringify({
                meta: {
                    topic: 'resource_change',
                    schema_uri: 'resource_change/1',
                    uri: 'https://en.wiktionary.org/api/rest_v1/page/html/Main_Page',
                    request_id: common.SAMPLE_REQUEST_ID,
                    id: uuid.now(),
                    dt: new Date().toISOString(),
                    domain: 'en.wiktionary.org'
                },
                tags: ['restbase']
            }))))
        .then(() => common.checkAPIDone(mwAPI))
        .finally(() => nock.cleanAll());
    });

    it('Should not react to revision change event from restbase for definition endpoint', () => {
        const mwAPI = nock('https://en.wiktionary.org')
        .get('/api/rest_v1/page/definition/Main_Page/12345')
        .query({ redirect: false })
        .reply(200, { });

        return P.try(() => producer.produce('test_dc.resource_change', 0,
            Buffer.from(JSON.stringify({
                meta: {
                    topic: 'resource_change',
                    schema_uri: 'resource_change/1',
                    uri: 'https://en.wiktionary.org/api/rest_v1/page/html/Main_Page/12345',
                    request_id: common.SAMPLE_REQUEST_ID,
                    id: uuid.now(),
                    dt: new Date().toISOString(),
                    domain: 'en.wiktionary.org'
                },
                tags: ['restbase']
            }))))
        .then(() => common.checkPendingMocks(mwAPI, 1))
        .finally(() => nock.cleanAll());
    });

    it('Should update mobile apps endpoint', () => {
        const mwAPI = nock('https://en.wikipedia.org', {
            reqheaders: {
                'cache-control': 'no-cache',
                'x-triggered-by': `req:${common.SAMPLE_REQUEST_ID},resource_change:https://en.wikipedia.org/api/rest_v1/page/html/Main_Page`,
                'user-agent': 'SampleChangePropInstance'
            }
        })
        .get('/api/rest_v1/page/mobile-sections/Main_Page')
        .query({ redirect: false })
        .reply(200, { });

        return P.try(() => producer.produce('test_dc.resource_change', 0, common.events.resourceChange().toBuffer()))
        .then(() => common.checkAPIDone(mwAPI))
        .finally(() => nock.cleanAll());
    });

    it('Should not update definition endpoint for non-main namespace', () => {
        const mwAPI = nock('https://en.wiktionary.org', {
            reqheaders: {
                'cache-control': 'no-cache',
                'user-agent': 'SampleChangePropInstance'
            }
        })
        .get('/api/rest_v1/page/definition/User%3APchelolo')
        .reply(200, () => {
            throw new Error('Update was made while it should not have');
        });

        return P.try(() => producer.produce('test_dc.resource_change', 0,
            common.events.resourceChange('https://en.wiktionary.org/api/rest_v1/page/html/User%3APchelolo').toBuffer()))
        .then(() => common.checkPendingMocks(mwAPI, 1))
        .finally(() => nock.cleanAll());
    });

    it('Should update RESTBase on resource_change from MW', () => {
        const mwAPI = nock('https://en.wikipedia.org', {
            reqheaders: {
                'cache-control': 'no-cache',
                'x-triggered-by': `req:${common.SAMPLE_REQUEST_ID},resource_change:https://en.wikipedia.org/wiki/Main_Page`,
                'if-unmodified-since': 'Tue, 20 Feb 1990 19:31:13 +0000',
                'user-agent': 'SampleChangePropInstance'
            }
        })
        .get('/api/rest_v1/page/html/Main_Page')
        .query({ redirect: false })
        .reply(200, { });

        return P.try(() => producer.produce('test_dc.resource_change', 0,
            common.events.resourceChange(
                'https://en.wikipedia.org/wiki/Main_Page',
                '1990-02-20T19:31:13+00:00',
                ['purge']
            ).toBuffer()))
        .then(() => common.checkAPIDone(mwAPI))
        .finally(() => nock.cleanAll());
    });

    it('Should update RESTBase on revision create', () => {
        const mwAPI = nock('https://en.wikipedia.org', {
            reqheaders: {
                'cache-control': 'no-cache',
                'x-triggered-by': `req:${common.SAMPLE_REQUEST_ID},mediawiki.revision-create:/edit/uri`,
                'x-restbase-parentrevision': '1233',
                'if-unmodified-since': 'Thu, 01 Jan 1970 00:00:01 +0000',
                'user-agent': 'SampleChangePropInstance'
            }
        })
        .get('/api/rest_v1/page/html/User%3APchelolo%2FTest/1234')
        .query({ redirect: false })
        .reply(200, { });

        return P.try(() => producer.produce('test_dc.mediawiki.revision-create', 0,
            Buffer.from(JSON.stringify({
                meta: {
                    topic: 'mediawiki.revision-create',
                    schema_uri: 'revision-create/1',
                    uri: '/edit/uri',
                    request_id: common.SAMPLE_REQUEST_ID,
                    id: uuid.now(),
                    dt: new Date(1000).toISOString(),
                    domain: 'en.wikipedia.org'
                },
                page_title: 'User:Pchelolo/Test',
                rev_id: 1234,
                rev_timestamp: new Date().toISOString(),
                rev_parent_id: 1233,
                rev_content_changed: true
            }))))
        .then(() => common.checkAPIDone(mwAPI))
        .finally(() => nock.cleanAll());
    });

    it('Should not update RESTBase on revision create for a blacklisted title', () => {
        const mwAPI = nock('https://en.wikipedia.org', {
            reqheaders: {
                'cache-control': 'no-cache',
                'x-triggered-by': `req:${common.SAMPLE_REQUEST_ID},mediawiki.revision-create:https://en.wikipedia.org/wiki/User:Nolelover`,
                'x-restbase-parentrevision': '1233',
                'if-unmodified-since': 'Thu, 01 Jan 1970 00:00:01 +0000',
                'user-agent': 'SampleChangePropInstance'
            }
        })
        .get('/api/rest_v1/page/html/User%3ANolelover/1234')
        .query({ redirect: false })
        .reply(200, { });

        return P.try(() => producer.produce('test_dc.mediawiki.revision-create', 0,
            Buffer.from(JSON.stringify({
                meta: {
                    topic: 'mediawiki.revision-create',
                    schema_uri: 'revision-create/1',
                    uri: 'https://en.wikipedia.org/wiki/User:Nolelover',
                    request_id: common.SAMPLE_REQUEST_ID,
                    id: uuid.now(),
                    dt: new Date(1000).toISOString(),
                    domain: 'en.wikipedia.org'
                },
                page_title: 'User:Nolelover',
                rev_id: 1234,
                rev_timestamp: new Date().toISOString(),
                rev_parent_id: 1233,
                rev_content_changed: true
            }))))
        .then(() => common.checkPendingMocks(mwAPI, 1))
        .finally(() => nock.cleanAll());
    });

    it('Should not update RESTBase on revision create for wikidata', () => {
        const mwAPI = nock('https://www.wikidata.org')
        .get('/api/rest_v1/page/html/Q1/1234')
        .query({ redirect: false })
        .reply(200, { });

        return P.try(() => producer.produce('test_dc.mediawiki.revision-create', 0,
            Buffer.from(JSON.stringify({
                meta: {
                    topic: 'mediawiki.revision-create',
                    schema_uri: 'revision-create/1',
                    uri: '/edit/uri',
                    request_id: common.SAMPLE_REQUEST_ID,
                    id: uuid.now(),
                    dt: new Date(1000).toISOString(),
                    domain: 'www.wikidata.org'
                },
                page_title: 'Q1',
                rev_id: 1234,
                rev_timestamp: new Date().toISOString(),
                rev_parent_id: 1233,
                page_namespace: 0,
                rev_content_changed: true
            }))))
        .then(() => common.checkPendingMocks(mwAPI, 1))
        .finally(() => nock.cleanAll());
    });

    it('Should update RESTBase on page delete', () => {
        const mwAPI = nock('https://en.wikipedia.org', {
            reqheaders: {
                'cache-control': 'no-cache',
                'x-triggered-by': `req:${common.SAMPLE_REQUEST_ID},mediawiki.page-delete:/delete/uri`,
                'user-agent': 'SampleChangePropInstance'
            }
        })
        .get('/api/rest_v1/page/title/User%3APchelolo%2FTest')
        .query({ redirect: false })
        .reply(200, { });

        return P.try(() => producer.produce('test_dc.mediawiki.page-delete', 0,
            Buffer.from(JSON.stringify({
                meta: {
                    topic: 'mediawiki.page-delete',
                    schema_uri: 'page_delete/1',
                    uri: '/delete/uri',
                    request_id: common.SAMPLE_REQUEST_ID,
                    id: uuid.now(),
                    dt: new Date().toISOString(),
                    domain: 'en.wikipedia.org'
                },
                page_title: 'User:Pchelolo/Test'
            }))))
        .then(() => common.checkAPIDone(mwAPI))
        .finally(() => nock.cleanAll());
    });

    it('Should update RESTBase on page undelete', () => {
        const mwAPI = nock('https://en.wikipedia.org', {
            reqheaders: {
                'cache-control': 'no-cache',
                'x-triggered-by': `req:${common.SAMPLE_REQUEST_ID},mediawiki.page-undelete:/restore/uri`,
                'user-agent': 'SampleChangePropInstance'
            }
        })
        .get('/api/rest_v1/page/title/User%3APchelolo%2FTest')
        .query({ redirect: false })
        .reply(200, { });

        return P.try(() => producer.produce('test_dc.mediawiki.page-undelete', 0,
            Buffer.from(JSON.stringify({
                meta: {
                    topic: 'mediawiki.page-undelete',
                    schema_uri: 'page_restore/1',
                    uri: '/restore/uri',
                    request_id: common.SAMPLE_REQUEST_ID,
                    id: uuid.now(),
                    dt: new Date().toISOString(),
                    domain: 'en.wikipedia.org'
                },
                page_title: 'User:Pchelolo/Test'
            }))))
        .then(() => common.checkAPIDone(mwAPI))
        .finally(() => nock.cleanAll());
    });

    it('Should update RESTBase on page move', () => {
        const mwAPI = nock('https://en.wikipedia.org', {
            reqheaders: {
                'cache-control': 'no-cache',
                'x-triggered-by': `req:${common.SAMPLE_REQUEST_ID},mediawiki.page-move:/move/uri`,
                'user-agent': 'SampleChangePropInstance'
            }
        })
        .get('/api/rest_v1/page/html/User%3APchelolo%2FTest1/2')
        .matchHeader( 'if-unmodified-since', 'Thu, 01 Jan 1970 00:00:01 +0000')
        .query({ redirect: false })
        .reply(200, { })
        .get('/api/rest_v1/page/title/User%3APchelolo%2FTest')
        .query({ redirect: false })
        .reply(200, { });

        return P.try(() => producer.produce('test_dc.mediawiki.page-move', 0,
            Buffer.from(JSON.stringify({
                meta: {
                    topic: 'mediawiki.page-move',
                    schema_uri: 'page_move/1',
                    uri: '/move/uri',
                    request_id: common.SAMPLE_REQUEST_ID,
                    id: uuid.now(),
                    dt: new Date(1000).toISOString(),
                    domain: 'en.wikipedia.org'
                },
                page_title: 'User:Pchelolo/Test1',
                rev_id: 2,
                prior_state: {
                    page_title: 'User:Pchelolo/Test'
                }
            }))))
        .then(() => common.checkAPIDone(mwAPI))
        .finally(() => nock.cleanAll());
    });

    it('Should update RESTBase on revision visibility change', () => {
        const mwAPI = nock('https://en.wikipedia.org', {
            reqheaders: {
                'cache-control': 'no-cache',
                'x-triggered-by': `req:${common.SAMPLE_REQUEST_ID},mediawiki.revision-visibility-change:/rev/uri`,
                'user-agent': 'SampleChangePropInstance'
            }
        })
        .get('/api/rest_v1/page/title/Foo/1234')
        .query({ redirect: false })
        .reply(200, { });

        return P.try(() => producer.produce('test_dc.mediawiki.revision-visibility-change', 0,
            Buffer.from(JSON.stringify({
                meta: {
                    topic: 'mediawiki.revision-visibility-change',
                    schema_uri: 'revision_visibility_set/1',
                    uri: '/rev/uri',
                    request_id: common.SAMPLE_REQUEST_ID,
                    id: uuid.now(),
                    dt: new Date().toISOString(),
                    domain: 'en.wikipedia.org'
                },
                page_title: 'Foo',
                rev_id: 1234
            }))))
        .then(() => common.checkAPIDone(mwAPI))
        .finally(() => nock.cleanAll());
    });

    it('Should update ORES on revision-create', () => {
        return common.fetchEventValidator('mediawiki/revision/score')
        .then((validate) => {
            const oresService = nock('https://ores.wikimedia.org')
            .post('/v3/precache')
            .reply(200, {
                "enwiki": {
                    "models": {
                        "damaging": {
                            "version": "0.4.0"
                        }
                    },
                    "scores": {
                        "1234": {
                            "damaging": {
                                "score": {
                                    "prediction": false,
                                    "probability": {
                                        "false": 0.6166652256695712,
                                        "true": 0.38333477433042884
                                    }
                                }
                            }
                        }
                    }
                }
            });
            const eventBusService = nock('https://eventbus.stubfortests.org')
            .post('/v1/events', function(body) {
                if (!body || !Array.isArray(body) || !body.length) {
                    return false;
                }
                return validate(body[0]);
            })
            .reply(200, {});
            return producer.produce('test_dc.mediawiki.revision-create', 0,
                common.events.revisionCreate().toBuffer())
            .then(() => common.checkAPIDone(oresService))
            .then(() => common.checkAPIDone(eventBusService))
            .finally(() => nock.cleanAll());
        });
    });

    it('Should update ORES on revision-create, error', () => {
        return common.fetchEventValidator('mediawiki/revision/score')
        .then((validate) => {
            const oresService = nock('https://ores.wikimedia.org')
            .post('/v3/precache')
            .reply(200, {
                "enwiki": {
                    "models": {
                        "damaging": {
                            "version": "0.4.0"
                        }
                    },
                    "scores": {
                        "1234": {
                            "damaging": {
                                "error": {
                                    "type": 'Bla',
                                    "message": 'Something is terribly wrong'
                                }
                            }
                        }
                    }
                }
            });
            const eventBusService = nock('https://eventbus.stubfortests.org')
            .post('/v1/events', function(body) {
                if (!body || !Array.isArray(body) || !body.length) {
                    return false;
                }
                return validate(body[0]);
            })
            .reply(200, {});
            return producer.produce('test_dc.mediawiki.revision-create', 0,
                common.events.revisionCreate().toBuffer())
            .then(() => common.checkAPIDone(oresService))
            .then(() => common.checkAPIDone(eventBusService))
            .finally(() => nock.cleanAll());
        });
    });

    it('Should update RESTBase summary and mobile-sections on wikidata description change', () => {
        const wikidataAPI = nock('https://www.wikidata.org')
        .post('/w/api.php', {
            format: 'json',
            formatversion: '2',
            action: 'wbgetentities',
            ids: 'Q1',
            props: 'sitelinks/urls',
            normalize: 'true'
        })
        .reply(200, {
            "success": 1,
            "entities": {
                "Q1": {
                    "type": "item",
                    "id": "Q1",
                    "sitelinks": {
                        "enwiki": {
                            "site": "ruwiki",
                            "title": "Пётр",
                            "badges": [],
                            "url": "https://ru.wikipedia.org/wiki/%D0%9F%D1%91%D1%82%D1%80"
                        }
                    }
                }
            }
        });

        const restbase = nock('https://ru.wikipedia.org', {
            reqheaders: {
                'cache-control': 'no-cache',
                'user-agent': 'SampleChangePropInstance',
                'x-triggered-by': `req:${common.SAMPLE_REQUEST_ID},mediawiki.revision-create:/rev/uri,change-prop.wikidata.resource-change:https://ru.wikipedia.org/wiki/%D0%9F%D1%91%D1%82%D1%80`
            }
        })
        .get('/api/rest_v1/page/summary/%D0%9F%D1%91%D1%82%D1%80')
        .query({ redirect: false })
        .reply(200, { })
        .get('/api/rest_v1/page/mobile-sections/%D0%9F%D1%91%D1%82%D1%80')
        .query({ redirect: false })
        .reply(200, { });

        return P.try(() => producer.produce('test_dc.mediawiki.revision-create', 0,
            Buffer.from(JSON.stringify({
                meta: {
                    topic: 'mediawiki.revision-create',
                    schema_uri: 'revision-create/1',
                    uri: '/rev/uri',
                    request_id: common.SAMPLE_REQUEST_ID,
                    id: uuid.now(),
                    dt: new Date().toISOString(),
                    domain: 'www.wikidata.org'
                },
                page_title: 'Q1',
                page_namespace: 0,
                comment: "/* wbeditentity-update:0| */ add [it] label",
                rev_content_changed: true
            }))))
        .delay(common.REQUEST_CHECK_DELAY)
        .then(() => common.checkAPIDone(wikidataAPI))
        .then(() => common.checkAPIDone(restbase))
        .finally(() => nock.cleanAll());
    });

    it('Should update RESTBase summary and mobile-sections on wikidata description revert', () => {
        const wikidataAPI = nock('https://www.wikidata.org')
        .post('/w/api.php', {
            format: 'json',
            formatversion: '2',
            action: 'wbgetentities',
            ids: 'Q1',
            props: 'sitelinks/urls',
            normalize: 'true'
        })
        .reply(200, {
            "success": 1,
            "entities": {
                "Q1": {
                    "type": "item",
                    "id": "Q1",
                    "sitelinks": {
                        "enwiki": {
                            "site": "ruwiki",
                            "title": "Пётр",
                            "badges": [],
                            "url": "https://ru.wikipedia.org/wiki/%D0%9F%D1%91%D1%82%D1%80"
                        }
                    }
                }
            }
        });

        const restbase = nock('https://ru.wikipedia.org', {
            reqheaders: {
                'cache-control': 'no-cache',
                'x-request-id': common.SAMPLE_REQUEST_ID,
                'user-agent': 'SampleChangePropInstance',
                'x-triggered-by': `req:${common.SAMPLE_REQUEST_ID},mediawiki.revision-create:/rev/uri,change-prop.wikidata.resource-change:https://ru.wikipedia.org/wiki/%D0%9F%D1%91%D1%82%D1%80`
            }
        })
        .get('/api/rest_v1/page/summary/%D0%9F%D1%91%D1%82%D1%80')
        .query({ redirect: false })
        .reply(200, { })
        .get('/api/rest_v1/page/mobile-sections/%D0%9F%D1%91%D1%82%D1%80')
        .query({ redirect: false })
        .reply(200, { });

        return P.try(() => producer.produce('test_dc.mediawiki.revision-create', 0,
            Buffer.from(JSON.stringify({
                meta: {
                    topic: 'mediawiki.revision-create',
                    schema_uri: 'revision-create/1',
                    uri: '/rev/uri',
                    request_id: common.SAMPLE_REQUEST_ID,
                    id: uuid.now(),
                    dt: new Date().toISOString(),
                    domain: 'www.wikidata.org'
                },
                page_title: 'Q1',
                page_namespace: 0,
                comment: "/* undo */ Undo revision 440223057 by Mhollo",
                rev_content_changed: true
            }))))
        .delay(common.REQUEST_CHECK_DELAY)
        .then(() => common.checkAPIDone(wikidataAPI))
        .then(() => common.checkAPIDone(restbase))
        .finally(() => nock.cleanAll());
    });

    it('Should update RESTBase summary and mobile-sections on wikidata undelete', () => {
        const wikidataAPI = nock('https://www.wikidata.org')
        .post('/w/api.php', {
            format: 'json',
            formatversion: '2',
            action: 'wbgetentities',
            ids: 'Q2',
            props: 'sitelinks/urls',
            normalize: 'true'
        })
        .reply(200, {
            "success": 1,
            "entities": {
                "Q2": {
                    "type": "item",
                    "id": "Q2",
                    "sitelinks": {
                        "enwiki": {
                            "site": "ruwiki",
                            "title": "Пётр",
                            "badges": [],
                            "url": "https://ru.wikipedia.org/wiki/%D0%9F%D1%91%D1%82%D1%80"
                        }
                    }
                }
            }
        });

        const restbase = nock('https://ru.wikipedia.org', {
            reqheaders: {
                'cache-control': 'no-cache',
                'user-agent': 'SampleChangePropInstance',
                'x-triggered-by': `req:${common.SAMPLE_REQUEST_ID},mediawiki.page-undelete:/rev/uri,change-prop.wikidata.resource-change:https://ru.wikipedia.org/wiki/%D0%9F%D1%91%D1%82%D1%80`
            }
        })
        .get('/api/rest_v1/page/summary/%D0%9F%D1%91%D1%82%D1%80')
        .query({ redirect: false })
        .reply(200, { })
        .get('/api/rest_v1/page/mobile-sections/%D0%9F%D1%91%D1%82%D1%80')
        .query({ redirect: false })
        .reply(200, { });

        return P.try(() => producer.produce('test_dc.mediawiki.page-undelete', 0,
            Buffer.from(JSON.stringify({
                meta: {
                    topic: 'mediawiki.page-undelete',
                    schema_uri: 'page-undelet/1',
                    uri: '/rev/uri',
                    request_id: common.SAMPLE_REQUEST_ID,
                    id: uuid.now(),
                    dt: new Date().toISOString(),
                    domain: 'www.wikidata.org'
                },
                page_title: 'Q2',
                page_namespace: 0,
            }))))
        .delay(common.REQUEST_CHECK_DELAY)
        .then(() => common.checkAPIDone(wikidataAPI))
        .then(() => common.checkAPIDone(restbase))
        .finally(() => nock.cleanAll());
    });

    it('Should not ask Wikidata for info for non-main namespace titles', () => {
        const wikidataAPI = nock('https://www.wikidata.org')
        .post('/w/api.php', {
            format: 'json',
            formatversion: '2',
            action: 'wbgetentities',
            ids: 'Property:P1',
            props: 'sitelinks/urls',
            normalize: 'true'
        })
        .reply(200, {
            "error": {
                "docref": "See https://www.wikidata.org/w/api.php for API usage",
                "messages": [{
                    "html": "Could not find such an entity.",
                    "parameters": [],
                    "name": "wikibase-api-no-such-entity"
                }],
                "id": "Property:P1",
                "info": "Could not find such an entity. (Invalid id: Property:1)",
                "code": "no-such-entity"
            },
        });

        return P.try(() => producer.produce('test_dc.mediawiki.revision-create', 0,
            Buffer.from(JSON.stringify({
                meta: {
                    topic: 'mediawiki.revision-create',
                    schema_uri: 'revision-create/1',
                    uri: '/rev/uri',
                    request_id: common.SAMPLE_REQUEST_ID,
                    id: uuid.now(),
                    dt: new Date().toISOString(),
                    domain: 'www.wikidata.org'
                },
                page_title: 'Property:P1',
                page_namespace: 3,
                rev_content_changed: true
            }))))
        .delay(common.REQUEST_CHECK_DELAY)
        .then(() => common.checkPendingMocks(wikidataAPI, 1))
        .finally(() => nock.cleanAll());
    });

    it('Should not crash if wikidata description can not be found', () => {
        const wikidataAPI = nock('https://www.wikidata.org')
        .post('/w/api.php', {
            format: 'json',
            formatversion: '2',
            action: 'wbgetentities',
            ids: 'Q2',
            props: 'sitelinks/urls',
            normalize: 'true'
        })
        .reply(200, {
            "entities": {
                "Q1220694122": {
                    "id": "Q1220694122",
                    "missing": ""
                }
            },
            "success": 1
        });

        return P.try(() => producer.produce('test_dc.mediawiki.revision-create', 0,
            Buffer.from(JSON.stringify({
                meta: {
                    topic: 'mediawiki.revision-create',
                    schema_uri: 'revision-create/1',
                    uri: '/rev/uri',
                    request_id: common.SAMPLE_REQUEST_ID,
                    id: uuid.now(),
                    dt: new Date().toISOString(),
                    domain: 'www.wikidata.org'
                },
                page_title: 'Q2',
                page_namespace: 0,
                comment: "/* wbeditentity-update:0| */ add [it] label",
                rev_content_changed: true
            }))))
        .delay(common.REQUEST_CHECK_DELAY)
        .then(() => common.checkAPIDone(wikidataAPI))
        .finally(() => nock.cleanAll());
    });

    it('Should rerender image usages on file update', () => {
        const mwAPI = nockWithOptionalSiteInfo()
        .get('/api/rest_v1/page/html/File%3APchelolo%2FTest.jpg/112233')
        .query({redirect: false})
        .reply(200)
        .post('/w/api.php', {
            format: 'json',
            action: 'query',
            list: 'imageusage',
            iutitle: 'File:Pchelolo/Test.jpg',
            iulimit: '500',
            formatversion: '2'
        })
        .reply(200, {
            batchcomplete: '',
            continue: {
                iucontinue: '1|2272',
                continue: '-||'
            },
            query: {
                imageusage: common.arrayWithLinks('File_Transcluded_Page', 2)
            }
        })
        .get('/api/rest_v1/page/html/File_Transcluded_Page')
        .query({redirect: false})
        .matchHeader('x-triggered-by', `req:${common.SAMPLE_REQUEST_ID},mediawiki.revision-create:https://en.wikipedia.org/wiki/SamplePage,change-prop.transcludes.resource-change:https://en.wikipedia.org/wiki/File_Transcluded_Page`)
        .matchHeader('if-unmodified-since', 'Tue, 20 Feb 1990 19:31:13 +0000')
        .matchHeader('x-restbase-mode', 'files')
        .times(2)
        .reply(200)
        .post('/w/api.php', {
            format: 'json',
            action: 'query',
            list: 'imageusage',
            iutitle: 'File:Pchelolo/Test.jpg',
            iulimit: '500',
            iucontinue: '1|2272',
            formatversion: '2'
        })
        .reply(200, {
            batchcomplete: '',
            query: {
                imageusage: common.arrayWithLinks('File_Transcluded_Page', 1)
            }
        })
        .get('/api/rest_v1/page/html/File_Transcluded_Page')
        .query({redirect: false})
        .matchHeader('x-triggered-by', `req:${common.SAMPLE_REQUEST_ID},mediawiki.revision-create:https://en.wikipedia.org/wiki/SamplePage,change-prop.transcludes.resource-change:https://en.wikipedia.org/wiki/File_Transcluded_Page`)
        .matchHeader('if-unmodified-since', 'Tue, 20 Feb 1990 19:31:13 +0000')
        .matchHeader('x-restbase-mode', 'files')
        .reply(200);

        return P.try(() => producer.produce('test_dc.mediawiki.revision-create', 0,
            Buffer.from(JSON.stringify({
                meta: {
                    topic: 'mediawiki.revision-create',
                    schema_uri: 'schema/1',
                    uri: 'https://en.wikipedia.org/wiki/SamplePage',
                    request_id: common.SAMPLE_REQUEST_ID,
                    id: uuid.now(),
                    dt: '1990-02-20T19:31:13+00:00',
                    domain: 'en.wikipedia.org'
                },
                page_title: 'File:Pchelolo/Test.jpg',
                rev_parent_id: 12345, // Needed to avoid backlinks updates firing and interfering
                rev_id: 112233,
                rev_content_changed: true
            }))))
        .then(() => common.checkAPIDone(mwAPI, 50))
        .finally(() => nock.cleanAll());
    });

    it('Should rerender transclusions on page update', () => {
        const mwAPI = nockWithOptionalSiteInfo()
        .get('/api/rest_v1/page/html/Test_Page/112233')
        .query({redirect: false})
        .reply(200)
        .post('/w/api.php', {
            format: 'json',
            formatversion: '2',
            action: 'query',
            prop: 'transcludedin',
            tiprop: 'title',
            tishow: '!redirect',
            titles: 'Test_Page',
            tilimit: '500'
        })
        .reply(200, {
            batchcomplete: '',
            continue: {
                ticontinue: '1|2272',
                continue: '-||'
            },
            query: {
                pages: {
                    '12345': {
                        transcludedin: common.arrayWithLinks('Transcluded_Here', 2)
                    }
                }
            }
        })
        .get('/api/rest_v1/page/html/Transcluded_Here')
        .query({redirect: false})
        .matchHeader('x-triggered-by', `req:${common.SAMPLE_REQUEST_ID},mediawiki.revision-create:https://en.wikipedia.org/wiki/SamplePage,change-prop.transcludes.resource-change:https://en.wikipedia.org/wiki/Transcluded_Here`)
        .matchHeader('if-unmodified-since', 'Tue, 20 Feb 1990 19:31:13 +0000')
        .matchHeader('x-restbase-mode', 'templates')
        .times(2)
        .reply(200)
        .post('/w/api.php', {
            format: 'json',
            formatversion: '2',
            action: 'query',
            prop: 'transcludedin',
            tiprop: 'title',
            tishow: '!redirect',
            titles: 'Test_Page',
            tilimit: '500',
            ticontinue: '1|2272'
        })
        .reply(200, {
            batchcomplete: '',
            query: {
                pages: {
                    '12345': {
                        transcludedin: common.arrayWithLinks('Transcluded_Here', 1)
                    }
                }
            }
        })
        .get('/api/rest_v1/page/html/Transcluded_Here')
        .query({redirect: false})
        .matchHeader('x-triggered-by', `req:${common.SAMPLE_REQUEST_ID},mediawiki.revision-create:https://en.wikipedia.org/wiki/SamplePage,change-prop.transcludes.resource-change:https://en.wikipedia.org/wiki/Transcluded_Here`)
        .matchHeader('if-unmodified-since', 'Tue, 20 Feb 1990 19:31:13 +0000')
        .matchHeader('x-restbase-mode', 'templates')
        .reply(200);

        return P.try(() => producer.produce('test_dc.mediawiki.revision-create', 0,
            Buffer.from(JSON.stringify({
                meta: {
                    topic: 'mediawiki.revision-create',
                    schema_uri: 'schema/1',
                    uri: 'https://en.wikipedia.org/wiki/SamplePage',
                    request_id: common.SAMPLE_REQUEST_ID,
                    id: uuid.now(),
                    dt: '1990-02-20T19:31:13+00:00',
                    domain: 'en.wikipedia.org'
                },
                page_title: 'Test_Page',
                rev_parent_id: 12345, // Needed to avoid backlinks updates firing and interfering
                rev_id: 112233,
                rev_content_changed: true
            }))))
        .then(() => common.checkAPIDone(mwAPI, 50))
        .finally(() => nock.cleanAll());
    });

    function backlinksTest(page_title, topic) {
        const mwAPI = nockWithOptionalSiteInfo()
            .get(`/api/rest_v1/page/title/${page_title}`)
            .query({ redirect: false })
            .optionally()
            .reply(200)
            .post('/w/api.php', {
                format: 'json',
                action: 'query',
                list: 'backlinks',
                bltitle: page_title,
                blfilterredir: 'nonredirects',
                bllimit: '500',
                formatversion: '2'
            })
            .reply(200, {
                batchcomplete: '',
                continue: {
                    blcontinue: '1|2272',
                    continue: '-||'
                },
                query: {
                    backlinks: common.arrayWithLinks(`Linked_${page_title}`, 2)
                }
            })
            .get(`/api/rest_v1/page/html/Linked_${page_title}`)
            .times(2)
            .query({ redirect: false })
            .matchHeader('x-triggered-by', `req:${common.SAMPLE_REQUEST_ID},${topic}:https://en.wikipedia.org/wiki/SamplePage,change-prop.backlinks.resource-change:https://en.wikipedia.org/wiki/Linked_${page_title}`)
            .reply(200)
            .post('/w/api.php', {
                format: 'json',
                action: 'query',
                list: 'backlinks',
                bltitle: page_title,
                blfilterredir: 'nonredirects',
                bllimit: '500',
                blcontinue: '1|2272',
                formatversion: '2'
            })
            .reply(200, {
                batchcomplete: '',
                query: {
                    backlinks: common.arrayWithLinks(`Linked_${page_title}`, 1)
                }
            })
            .get(`/api/rest_v1/page/html/Linked_${page_title}`)
            .query({ redirect: false })
            .matchHeader('x-triggered-by', `req:${common.SAMPLE_REQUEST_ID},${topic}:https://en.wikipedia.org/wiki/SamplePage,change-prop.backlinks.resource-change:https://en.wikipedia.org/wiki/Linked_${page_title}`)
            .reply(200);

        return P.try(() => producer.produce(`test_dc.${topic}`, 0,
            Buffer.from(JSON.stringify(common.eventWithProperties(topic, { page_title })))))
            .then(() => common.checkAPIDone(mwAPI, 50))
            .finally(() => nock.cleanAll());
    }

    it('Should process backlinks, on create', () => backlinksTest('On_Create', 'mediawiki.page-create'));
    it('Should process backlinks, on delete', () => backlinksTest('On_Delete', 'mediawiki.page-delete'));
    it('Should process backlinks, on undelete', () => backlinksTest('On_Undelete', 'mediawiki.page-undelete'));


    it('Should purge caches on resource_change coming from RESTBase', (done) => {
        return testPurgeCacheOnResourceChange(
            'http://en.wikipedia.beta.wmflabs.org/api/rest_v1/page/html/User%3APchelolo%2FTest/331536',
            'http://en.wikipedia.beta.wmflabs.org/api/rest_v1/page/html/User%3APchelolo%2FTest/331536',
            'en.wikipedia.beta.wmflabs.org',
            ['restbase'],
            'User%3APchelolo%2FTest',
            done
        );
    });

    it('Should purge caches on resource_change coming from Tilerator', (done) => {
        return testPurgeCacheOnResourceChange(
            'https://maps-beta.wmflabs.org/osm-intl/12/2074/1405.png',
            'http://maps-beta.wmflabs.org/osm-intl/12/2074/1405.png',
            'maps-beta.wmflabs.org',
            ['tilerator'],
            'osm-intl',
            done
        );
    });

    after(() => changeProp.stop());
});
