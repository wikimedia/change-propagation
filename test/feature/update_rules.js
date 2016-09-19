"use strict";

const ChangeProp = require('../utils/changeProp');
const nock = require('nock');
const uuid = require('cassandra-uuid').TimeUuid;
const common = require('../utils/common');
const dgram  = require('dgram');
const assert = require('assert');

process.env.UV_THREADPOOL_SIZE = 128;

describe('RESTBase update rules', function() {
    this.timeout(30000);

    const changeProp = new ChangeProp('config.example.wikimedia.yaml');
    let producer;

    before(function() {
        // Setting up might take some tome, so disable the timeout
        this.timeout(50000);
        return changeProp.start()
        .then(() =>  common.factory.createProducer())
        .then((result) => producer = result);
    });

    it('Should update summary endpoint', () => {
        const mwAPI = nock('https://en.wikipedia.org', {
            reqheaders: {
                'cache-control': 'no-cache',
                'x-triggered-by': 'resource_change:https://en.wikipedia.org/api/rest_v1/page/html/Main_Page',
                'x-request-id': common.SAMPLE_REQUEST_ID,
                'user-agent': 'SampleChangePropInstance'
            }
        })
        .get('/api/rest_v1/page/summary/Main_Page')
        .query({ redirect: false })
        .reply(200, { });

        return producer.produceAsync({
            topic: 'test_dc.resource_change',
            message: JSON.stringify({
                meta: {
                    topic: 'resource_change',
                    schema_uri: 'resource_change/1',
                    uri: 'https://en.wikipedia.org/api/rest_v1/page/html/Main_Page',
                    request_id: common.SAMPLE_REQUEST_ID,
                    id: uuid.now(),
                    dt: new Date().toISOString(),
                    domain: 'en.wikipedia.org'
                },
                tags: ['restbase']
            })
        })
        .then(() => common.checkAPIDone(mwAPI))
        .finally(() => nock.cleanAll());
    });

    it('Should update definition endpoint', () => {
        const mwAPI = nock('https://en.wiktionary.org', {
            reqheaders: {
                'cache-control': 'no-cache',
                'x-triggered-by': 'resource_change:https://en.wiktionary.org/api/rest_v1/page/html/Main_Page',
                'x-request-id': common.SAMPLE_REQUEST_ID,
                'user-agent': 'SampleChangePropInstance'
            }
        })
        .get('/api/rest_v1/page/definition/Main_Page')
        .query({ redirect: false })
        .reply(200, {});

        return producer.produceAsync({
            topic: 'test_dc.resource_change',
            message: JSON.stringify({
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
            })
        })
        .then(() => common.checkAPIDone(mwAPI))
        .finally(() => nock.cleanAll());
    });

    it('Should not react to revision change event from restbase for definition endpoint', () => {
        const mwAPI = nock('https://en.wiktionary.org')
        .get('/api/rest_v1/page/definition/Main_Page/12345')
        .query({ redirect: false })
        .reply(200, { });

        return producer.produceAsync({
            topic: 'test_dc.resource_change',
            message: JSON.stringify({
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
            })
        })
        .then(() => common.checkPendingMocks(mwAPI, 1))
        .finally(() => nock.cleanAll());
    });

    it('Should update mobile apps endpoint', () => {
        const mwAPI = nock('https://en.wikipedia.org', {
            reqheaders: {
                'cache-control': 'no-cache',
                'x-triggered-by': 'resource_change:https://en.wikipedia.org/api/rest_v1/page/html/Main_Page',
                'x-request-id': common.SAMPLE_REQUEST_ID,
                'user-agent': 'SampleChangePropInstance'
            }
        })
        .get('/api/rest_v1/page/mobile-sections/Main_Page')
        .query({ redirect: false })
        .reply(200, { });

        return producer.produceAsync({
            topic: 'test_dc.resource_change',
            message: JSON.stringify({
                meta: {
                    topic: 'resource_change',
                    schema_uri: 'resource_change/1',
                    uri: 'https://en.wikipedia.org/api/rest_v1/page/html/Main_Page',
                    request_id: common.SAMPLE_REQUEST_ID,
                    id: uuid.now(),
                    dt: new Date().toISOString(),
                    domain: 'en.wikipedia.org'
                },
                tags: ['restbase']
            })
        })
        .then(() => common.checkAPIDone(mwAPI))
        .finally(() => nock.cleanAll());
    });

    it('Should not update definition endpoint for non-main namespace', () => {
        const mwAPI = nock('https://en.wiktionary.org', {
            reqheaders: {
                'cache-control': 'no-cache',
                'x-request-id': common.SAMPLE_REQUEST_ID,
                'user-agent': 'SampleChangePropInstance'
            }
        })
        .get('/api/rest_v1/page/definition/User%3APchelolo')
        .reply(200, () => {
            throw new Error('Update was made while it should not have');
        });

        return producer.produceAsync({
            topic: 'test_dc.resource_change',
            message: JSON.stringify({
                meta: {
                    topic: 'resource_change',
                    schema_uri: 'resource_change/1',
                    uri: 'https://en.wiktionary.org/api/rest_v1/page/html/User%3APchelolo',
                    request_id: common.SAMPLE_REQUEST_ID,
                    id: uuid.now(),
                    dt: new Date().toISOString(),
                    domain: 'en.wiktionary.org'
                },
                tags: ['restbase']
            })
        })
        .then(() => common.checkPendingMocks(mwAPI, 1))
        .finally(() => nock.cleanAll());
    });

    it('Should update RESTBase on resource_change from MW', () => {
        const mwAPI = nock('https://en.wikipedia.org', {
            reqheaders: {
                'cache-control': 'no-cache',
                'x-triggered-by': 'resource_change:https://en.wikipedia.org/wiki/Main_Page',
                'x-request-id': common.SAMPLE_REQUEST_ID,
                'if-unmodified-since': 'Tue, 20 Feb 1990 19:31:13 +0000',
                'user-agent': 'SampleChangePropInstance'
            }
        })
        .get('/api/rest_v1/page/html/Main_Page')
        .query({ redirect: false })
        .reply(200, { });

        return producer.produceAsync({
            topic: 'test_dc.resource_change',
            message: JSON.stringify({
                meta: {
                    topic: 'resource_change',
                    schema_uri: 'resource_change/1',
                    uri: 'https://en.wikipedia.org/wiki/Main_Page',
                    request_id: common.SAMPLE_REQUEST_ID,
                    id: uuid.now(),
                    dt: '1990-02-20T19:31:13+00:00',
                    domain: 'en.wikipedia.org'
                },
                tags: ['purge']
            })
        })
        .then(() => common.checkAPIDone(mwAPI))
        .finally(() => nock.cleanAll());
    });

    it('Should update RESTBase on revision create', () => {
        const mwAPI = nock('https://en.wikipedia.org', {
            reqheaders: {
                'cache-control': 'no-cache',
                'x-triggered-by': 'mediawiki.revision-create:/edit/uri',
                'x-request-id': common.SAMPLE_REQUEST_ID,
                'x-restbase-parentrevision': '1233',
                'if-unmodified-since': 'Thu, 01 Jan 1970 00:00:01 +0000',
                'user-agent': 'SampleChangePropInstance'
            }
        })
        .get('/api/rest_v1/page/html/User%3APchelolo%2FTest/1234')
        .query({ redirect: false })
        .reply(200, { });

        return producer.produceAsync({
            topic: 'test_dc.mediawiki.revision-create',
            message: JSON.stringify({
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
                rev_parent_id: 1233
            })
        })
        .then(() => common.checkAPIDone(mwAPI))
        .finally(() => nock.cleanAll());
    });

    it('Should update RESTBase on page delete', () => {
        const mwAPI = nock('https://en.wikipedia.org', {
            reqheaders: {
                'cache-control': 'no-cache',
                'x-triggered-by': 'mediawiki.page-delete:/delete/uri',
                'x-request-id': common.SAMPLE_REQUEST_ID,
                'user-agent': 'SampleChangePropInstance'
            }
        })
        .get('/api/rest_v1/page/title/User%3APchelolo%2FTest')
        .query({ redirect: false })
        .reply(200, { });

        return producer.produceAsync({
            topic: 'test_dc.mediawiki.page-delete',
            message: JSON.stringify({
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
            })
        })
        .then(() => common.checkAPIDone(mwAPI))
        .finally(() => nock.cleanAll());
    });

    it('Should update RESTBase on page undelete', () => {
        const mwAPI = nock('https://en.wikipedia.org', {
            reqheaders: {
                'cache-control': 'no-cache',
                'x-triggered-by': 'mediawiki.page-undelete:/restore/uri',
                'x-request-id': common.SAMPLE_REQUEST_ID,
                'user-agent': 'SampleChangePropInstance'
            }
        })
        .get('/api/rest_v1/page/html/User%3APchelolo%2FTest')
        .query({ redirect: false })
        .reply(200, { });

        return producer.produceAsync({
            topic: 'test_dc.mediawiki.page-undelete',
            message: JSON.stringify({
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
            })
        })
        .then(() => common.checkAPIDone(mwAPI))
        .finally(() => nock.cleanAll());
    });

    it('Should update RESTBase on page move', () => {
        const mwAPI = nock('https://en.wikipedia.org', {
            reqheaders: {
                'cache-control': 'no-cache',
                'x-request-id': common.SAMPLE_REQUEST_ID,
                'x-triggered-by': 'mediawiki.page-move:/move/uri',
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

        return producer.produceAsync({
            topic: 'test_dc.mediawiki.page-move',
            message: JSON.stringify({
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
            })
        })
        .then(() => common.checkAPIDone(mwAPI))
        .finally(() => nock.cleanAll());
    });

    it('Should update RESTBase on revision visibility change', () => {
        const mwAPI = nock('https://en.wikipedia.org', {
            reqheaders: {
                'cache-control': 'no-cache',
                'x-triggered-by': 'mediawiki.revision-visibility-change:/rev/uri',
                'x-request-id': common.SAMPLE_REQUEST_ID,
                'user-agent': 'SampleChangePropInstance'
            }
        })
        .get('/api/rest_v1/page/revision/1234')
        .query({ redirect: false })
        .reply(200, { });

        return producer.produceAsync({
            topic: 'test_dc.mediawiki.revision-visibility-change',
            message: JSON.stringify({
                meta: {
                    topic: 'mediawiki.revision-visibility-change',
                    schema_uri: 'revision_visibility_set/1',
                    uri: '/rev/uri',
                    request_id: common.SAMPLE_REQUEST_ID,
                    id: uuid.now(),
                    dt: new Date().toISOString(),
                    domain: 'en.wikipedia.org'
                },
                rev_id: 1234
            })
        })
        .then(() => common.checkAPIDone(mwAPI))
        .finally(() => nock.cleanAll());
    });

    it('Should update ORES on revision-create', () => {
        const oresService = nock('https://ores.wikimedia.org')
        .get('/v2/scores/enwiki/')
        .query({
            models: 'reverted|damaging|goodfaith',
            revids: 1234,
            precache: true })
        .reply(200, { });

        return producer.produceAsync({
            topic: 'test_dc.mediawiki.revision-create',
            message: JSON.stringify({
                meta: {
                    topic: 'mediawiki.revision-create',
                    schema_uri: 'revision-create/1',
                    uri: '/edit/uri',
                    request_id: common.SAMPLE_REQUEST_ID,
                    id: uuid.now(),
                    dt: new Date(1000).toISOString(),
                    domain: 'en.wikipedia.org'
                },
                page_title: 'TestPage',
                rev_id: 1234,
                rev_timestamp: new Date().toISOString(),
                rev_parent_id: 1233,
                performer: {
                    user_is_bot: false
                },
                database: 'enwiki'
            })
        })
        .then(() => common.checkAPIDone(oresService))
        .finally(() => nock.cleanAll());
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
                'x-request-id': common.SAMPLE_REQUEST_ID,
                'user-agent': 'SampleChangePropInstance',
                'x-triggered-by': 'mediawiki.revision-create:/rev/uri,change-prop.transcludes.resource-change:https://ru.wikipedia.org/wiki/%D0%9F%D1%91%D1%82%D1%80'
            }
        })
        .get('/api/rest_v1/page/summary/%D0%9F%D1%91%D1%82%D1%80')
        .query({ redirect: false })
        .reply(200, { })
        .get('/api/rest_v1/page/mobile-sections/%D0%9F%D1%91%D1%82%D1%80')
        .query({ redirect: false })
        .reply(200, { });

        return producer.produceAsync({
            topic: 'test_dc.mediawiki.revision-create',
            message: JSON.stringify({
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
                page_namespace: 0
            })
        })
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

        return producer.produceAsync({
            topic: 'test_dc.mediawiki.revision-create',
            message: JSON.stringify({
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
                page_namespace: 3
            })
        })
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

        return producer.produceAsync({
            topic: 'test_dc.mediawiki.revision-create',
            message: JSON.stringify({
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
                page_namespace: 0
            })
        })
        .delay(common.REQUEST_CHECK_DELAY)
        .then(() => common.checkAPIDone(wikidataAPI))
        .finally(() => nock.cleanAll());
    });

    it('Should rerender image usages on file update', () => {
        const mwAPI = nock('https://en.wikipedia.org')
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
        .matchHeader('x-triggered-by', 'mediawiki.revision-create:/sample/uri,change-prop.transcludes.resource-change:https://en.wikipedia.org/wiki/File_Transcluded_Page')
        .matchHeader('if-unmodified-since', 'Tue, 20 Feb 1990 19:31:13 +0000')
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
        .matchHeader('x-triggered-by', 'mediawiki.revision-create:/sample/uri,change-prop.transcludes.resource-change:https://en.wikipedia.org/wiki/File_Transcluded_Page')
        .matchHeader('if-unmodified-since', 'Tue, 20 Feb 1990 19:31:13 +0000')
        .reply(200);

        return producer.produceAsync({
            topic: 'test_dc.mediawiki.revision-create',
            message: JSON.stringify({
                meta: {
                    topic: 'mediawiki.revision-create',
                    schema_uri: 'schema/1',
                    uri: '/sample/uri',
                    request_id: common.SAMPLE_REQUEST_ID,
                    id: uuid.now(),
                    dt: '1990-02-20T19:31:13+00:00',
                    domain: 'en.wikipedia.org'
                },
                page_title: 'File:Pchelolo/Test.jpg',
                rev_parent_id: 12345 // Needed to avoid backlinks updates firing and interfering
            })
        })
        .then(() => common.checkAPIDone(mwAPI, 50))
        .finally(() => nock.cleanAll());
    });

    it('Should rerender transclusions on page update', () => {
        const mwAPI = nock('https://en.wikipedia.org')
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
        .matchHeader('x-triggered-by', 'mediawiki.revision-create:/sample/uri,change-prop.transcludes.resource-change:https://en.wikipedia.org/wiki/Transcluded_Here')
        .matchHeader('if-unmodified-since', 'Tue, 20 Feb 1990 19:31:13 +0000')
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
        .matchHeader('x-triggered-by', 'mediawiki.revision-create:/sample/uri,change-prop.transcludes.resource-change:https://en.wikipedia.org/wiki/Transcluded_Here')
        .matchHeader('if-unmodified-since', 'Tue, 20 Feb 1990 19:31:13 +0000')
        .reply(200);

        return producer.produceAsync({
            topic: 'test_dc.mediawiki.revision-create',
            message: JSON.stringify({
                meta: {
                    topic: 'mediawiki.revision-create',
                    schema_uri: 'schema/1',
                    uri: '/sample/uri',
                    request_id: common.SAMPLE_REQUEST_ID,
                    id: uuid.now(),
                    dt: '1990-02-20T19:31:13+00:00',
                    domain: 'en.wikipedia.org'
                },
                page_title: 'Test_Page',
                rev_parent_id: 12345 // Needed to avoid backlinks updates firing and interfering
            })
        })
        .then(() => common.checkAPIDone(mwAPI, 50))
        .finally(() => nock.cleanAll());
    });

    it('Should purge caches on resource_change coming from RESTBase', (done) => {
        var udpServer = dgram.createSocket('udp4');
        let closed = false;
        udpServer.on("message", function(msg) {
            try {
                msg = msg.slice(22, 22 + msg.readInt16BE(20)).toString();
                if (msg.indexOf('User%3APchelolo%2FTest') >= 0) {
                    assert.deepEqual(msg,
                        'http://en.wikipedia.beta.wmflabs.org/api/rest_v1/page/html/User%3APchelolo%2FTest/331536')
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

        producer.produceAsync({
            topic: 'test_dc.resource_change',
            message: JSON.stringify({
                meta: {
                    topic: 'resource_change',
                    schema_uri: 'resource_change/1',
                    uri: 'http://en.wikipedia.beta.wmflabs.org/api/rest_v1/page/html/User%3APchelolo%2FTest/331536',
                    request_id: uuid.now(),
                    id: uuid.now(),
                    dt: new Date().toISOString(),
                    domain: 'en.wikipedia.beta.wmflabs.org'
                },
                tags: ['restbase']
            })
        })
        .delay(common.REQUEST_CHECK_DELAY)
        .finally(() => {
            if (!closed) {
                udpServer.close();
                done(new Error('Timeout!'));
            }
        });
    });

    it('Should process backlinks', () => {
        const mwAPI = nock('https://en.wikipedia.org')
        .post('/w/api.php', {
            format: 'json',
            action: 'query',
            list: 'backlinks',
            bltitle: 'User:Pchelolo/Test',
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
                backlinks: common.arrayWithLinks('Some_Page', 2)
            }
        })
        .get('/api/rest_v1/page/html/Some_Page')
        .matchHeader('x-triggered-by', 'mediawiki.revision-create:/sample/uri,change-prop.transcludes.resource-change:https://en.wikipedia.org/wiki/Some_Page')
        .times(2)
        .reply(200)
        .post('/w/api.php', {
            format: 'json',
            action: 'query',
            list: 'backlinks',
            bltitle: 'User:Pchelolo/Test',
            blfilterredir: 'nonredirects',
            bllimit: '500',
            blcontinue: '1|2272',
            formatversion: '2'
        })
        .reply(200, {
            batchcomplete: '',
            query: {
                backlinks: common.arrayWithLinks('Some_Page', 1)
            }
        })
        .get('/api/rest_v1/page/html/Some_Page')
        .matchHeader('x-triggered-by', 'mediawiki.revision-create:/sample/uri,change-prop.transcludes.resource-change:https://en.wikipedia.org/wiki/Some_Page')
        .reply(200);

        return producer.produceAsync({
            topic: 'test_dc.mediawiki.revision-create',
            message: JSON.stringify(common.eventWithProperties('mediawiki.revision-create', {
                page_title: 'User:Pchelolo/Test'
            }))
        })
        .then(() => common.checkAPIDone(mwAPI, 50))
        .finally(() => nock.cleanAll());
    });

    after(() => changeProp.stop());
});
