"use strict";

const ChangeProp = require('../utils/changeProp');
const kafka = require('rdkafka');
const nock = require('nock');
const uuid = require('cassandra-uuid').TimeUuid;
const common = require('../utils/common');
const dgram  = require('dgram');
const assert = require('assert');

describe('RESTBase update rules', function() {
    this.timeout(15000);

    const changeProp = new ChangeProp('config.example.wikimedia.yaml');
    let producer = new kafka.Producer({
        "metadata.broker.list": "127.0.0.1:9092",
        "queue.buffering.max.ms": "1"
    });

    before(function() {
        // Setting up might take some tome, so disable the timeout
        this.timeout(20000);
        return changeProp.start();
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

        return producer.produce('test_dc.resource_change',
            0,
            JSON.stringify({
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
            }))
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

        return producer.produce('test_dc.resource_change',
            0,
            JSON.stringify({
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
            }))
        .then(() => common.checkAPIDone(mwAPI))
        .finally(() => nock.cleanAll());
    });

    it('Should not react to revision change event from restbase for definition endpoint', () => {
        const mwAPI = nock('https://en.wiktionary.org')
        .get('/api/rest_v1/page/definition/Main_Page/12345')
        .query({ redirect: false })
        .reply(200, { });

        return producer.produce('test_dc.resource_change', 0, JSON.stringify({
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
        }))
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

        return producer.produce('test_dc.resource_change',
            0,
            JSON.stringify({
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
            }))
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

        return producer.produce('test_dc.resource_change',
            0,
            JSON.stringify({
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
            }))
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

        return producer.produce('test_dc.resource_change',
            0,
            JSON.stringify({
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
            }))
        .then(() => common.checkAPIDone(mwAPI))
        .finally(() => nock.cleanAll());
    });

    it('Should update RESTBase on revision_create', () => {
        const mwAPI = nock('https://en.wikipedia.org', {
            reqheaders: {
                'cache-control': 'no-cache',
                'x-triggered-by': 'mediawiki.revision_create:/edit/uri',
                'x-request-id': common.SAMPLE_REQUEST_ID,
                'x-restbase-parentrevision': '1233',
                'if-unmodified-since': 'Thu, 01 Jan 1970 00:00:01 +0000',
                'user-agent': 'SampleChangePropInstance'
            }
        })
        .get('/api/rest_v1/page/html/User%3APchelolo%2FTest/1234')
        .query({ redirect: false })
        .reply(200, { });

        return producer.produce('test_dc.mediawiki.revision_create',
            0,
            JSON.stringify({
                meta: {
                    topic: 'mediawiki.revision_create',
                    schema_uri: 'revision_create/1',
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
            }))
        .then(() => common.checkAPIDone(mwAPI))
        .finally(() => nock.cleanAll());
    });

    it('Should update RESTBase on page delete', () => {
        const mwAPI = nock('https://en.wikipedia.org', {
            reqheaders: {
                'cache-control': 'no-cache',
                'x-triggered-by': 'mediawiki.page_delete:/delete/uri',
                'x-request-id': common.SAMPLE_REQUEST_ID,
                'user-agent': 'SampleChangePropInstance'
            }
        })
        .get('/api/rest_v1/page/title/User%3APchelolo%2FTest')
        .query({ redirect: false })
        .reply(200, { });

        return producer.produce('test_dc.mediawiki.page_delete',
            0,
            JSON.stringify({
                meta: {
                    topic: 'mediawiki.page_delete',
                    schema_uri: 'page_delete/1',
                    uri: '/delete/uri',
                    request_id: common.SAMPLE_REQUEST_ID,
                    id: uuid.now(),
                    dt: new Date().toISOString(),
                    domain: 'en.wikipedia.org'
                },
                title: 'User:Pchelolo/Test'
            }))
        .then(() => common.checkAPIDone(mwAPI))
        .finally(() => nock.cleanAll());
    });

    it('Should update RESTBase on page_restore', () => {
        const mwAPI = nock('https://en.wikipedia.org', {
            reqheaders: {
                'cache-control': 'no-cache',
                'x-triggered-by': 'mediawiki.page_restore:/restore/uri',
                'x-request-id': common.SAMPLE_REQUEST_ID,
                'user-agent': 'SampleChangePropInstance'
            }
        })
        .get('/api/rest_v1/page/html/User%3APchelolo%2FTest')
        .query({ redirect: false })
        .reply(200, { });

        return producer.produce('test_dc.mediawiki.page_restore',
            0,
            JSON.stringify({
                meta: {
                    topic: 'mediawiki.page_restore',
                    schema_uri: 'page_restore/1',
                    uri: '/restore/uri',
                    request_id: common.SAMPLE_REQUEST_ID,
                    id: uuid.now(),
                    dt: new Date().toISOString(),
                    domain: 'en.wikipedia.org'
                },
                title: 'User:Pchelolo/Test'
            }))
        .then(() => common.checkAPIDone(mwAPI))
        .finally(() => nock.cleanAll());
    });

    it('Should update ORES on revision_create', () => {
        const oresService = nock('https://ores.wikimedia.org')
        .get('/v2/scores/enwiki/')
        .query({
            models: 'reverted|damaging|goodfaith',
            revids: 1234,
            precache: true })
        .reply(200, { });

        return producer.produce('test_dc.mediawiki.revision_create',
            0,
            JSON.stringify({
                meta: {
                    topic: 'mediawiki.revision_create',
                    schema_uri: 'revision_create/1',
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
                rev_by_bot: false
            }))
        .then(() => common.checkAPIDone(oresService))
        .finally(() => nock.cleanAll());
    });

    it('Should update RESTBase on page move', () => {
        const mwAPI = nock('https://en.wikipedia.org', {
            reqheaders: {
                'cache-control': 'no-cache',
                'x-request-id': common.SAMPLE_REQUEST_ID,
                'x-triggered-by': 'mediawiki.page_move:/move/uri',
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

        return producer.produce('test_dc.mediawiki.page_move',
            0,
            JSON.stringify({
                meta: {
                    topic: 'mediawiki.page_move',
                    schema_uri: 'page_move/1',
                    uri: '/move/uri',
                    request_id: common.SAMPLE_REQUEST_ID,
                    id: uuid.now(),
                    dt: new Date(1000).toISOString(),
                    domain: 'en.wikipedia.org'
                },
                old_title: 'User:Pchelolo/Test',
                new_title: 'User:Pchelolo/Test1',
                old_revision_id: 1,
                new_revision_id: 2
            }))
        .then(() => common.checkAPIDone(mwAPI))
        .finally(() => nock.cleanAll());
    });

    it('Should update RESTBase on revision visibility change', () => {
        const mwAPI = nock('https://en.wikipedia.org', {
            reqheaders: {
                'cache-control': 'no-cache',
                'x-triggered-by': 'mediawiki.revision_visibility_set:/rev/uri',
                'x-request-id': common.SAMPLE_REQUEST_ID,
                'user-agent': 'SampleChangePropInstance'
            }
        })
        .get('/api/rest_v1/page/revision/1234')
        .query({ redirect: false })
        .reply(200, { });

        return producer.produce('test_dc.mediawiki.revision_visibility_set',
            0,
            JSON.stringify({
                meta: {
                    topic: 'mediawiki.revision_visibility_set',
                    schema_uri: 'revision_visibility_set/1',
                    uri: '/rev/uri',
                    request_id: common.SAMPLE_REQUEST_ID,
                    id: uuid.now(),
                    dt: new Date().toISOString(),
                    domain: 'en.wikipedia.org'
                },
                revision_id: 1234
            }))
        .then(() => common.checkAPIDone(mwAPI))
        .finally(() => nock.cleanAll());
    });


    it.skip('Should rerender image usages on file update', () => {
        const mwAPI = nock('https://en.wikipedia.org')
        .post('/w/api.php', {
            format: 'json',
            action: 'query',
            meta: 'siteinfo',
            siprop: 'general|namespaces|namespacealiases'
        })
        .reply(200, common.EN_SITE_INFO_RESPONSE)
        .post('/w/api.php', {
            format: 'json',
            action: 'query',
            list: 'imageusage',
            iutitle: 'File:Test.jpg',
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
                imageusage: common.arrayWithLinks('Some_Page', 2)
            }
        })
        .get('/api/rest_v1/page/html/Some_Page')
        .query({redirect: false})
        .matchHeader('x-triggered-by', 'mediawiki.revision_create:/sample/uri,resource_change:https://en.wikipedia.org/wiki/Some_Page')
        .times(2)
        .reply(200)
        .post('/w/api.php', {
            format: 'json',
            action: 'query',
            list: 'imageusage',
            iutitle: 'File:Test.jpg',
            iulimit: '500',
            iucontinue: '1|2272',
            formatversion: '2'
        })
        .reply(200, {
            batchcomplete: '',
            query: {
                imageusage: common.arrayWithLinks('Some_Page', 1)
            }
        })
        .get('/api/rest_v1/page/html/Some_Page')
        .query({redirect: false})
        .matchHeader('x-triggered-by', 'mediawiki.revision_create:/sample/uri,resource_change:https://en.wikipedia.org/wiki/Some_Page')
        .reply(200);

        return producer.produce('test_dc.mediawiki.revision_create',
            0,
            JSON.stringify(common.eventWithProperties('mediawiki.revision_create', { page_title: 'File:Test.jpg' })))
        .then(() => common.checkAPIDone(mwAPI))
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

        return producer.produce('test_dc.resource_change', 0,
            JSON.stringify({
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
            }))
        .delay(common.REQUEST_CHECK_DELAY)
        .finally(() => {
            if (!closed) {
                udpServer.close();
                done(new Error('Timeout!'));
            }
        });
    });

    after(() => {
        changeProp.stop();
        producer.close();
    });
});
