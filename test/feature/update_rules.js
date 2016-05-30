"use strict";

const ChangeProp = require('../utils/changeProp');
const KafkaFactory = require('../../lib/kafka_factory');
const nock = require('nock');
const uuid = require('cassandra-uuid').TimeUuid;
const common = require('../utils/common');
const dgram  = require('dgram');
const assert = require('assert');
const P = require('bluebird');

describe('RESTBase update rules', function() {
    this.timeout(2000);

    const changeProp = new ChangeProp('config.example.wikimedia.yaml');
    const kafkaFactory = new KafkaFactory({
        uri: 'localhost:2181/', // TODO: find out from the config
        clientId: 'change-prop-test-suite',
        dc_name: 'test_dc'
    });
    let producer;

    before(function() {
        // Setting up might tike some tome, so disable the timeout
        this.timeout(20000);

        return kafkaFactory.newProducer(kafkaFactory.newClient())
        .then((newProducer) => {
            producer = newProducer;
            if (!common.topics_created) {
                common.topics_created = true;
                return P.each(common.ALL_TOPICS, (topic) => {
                    return producer.createTopicsAsync([ topic ], false);
                });
            }
            return P.resolve();
        })
        .then(() => changeProp.start());
    });

    it('Should update summary endpoint', () => {
        const mwAPI = nock('https://en.wikipedia.org', {
            reqheaders: {
                'cache-control': 'no-cache',
                'x-triggered-by': 'resource_change:https://en.wikipedia.org/api/rest_v1/page/html/Main%20Page',
                'x-request-id': common.SAMPLE_REQUEST_ID,
                'user-agent': 'SampleChangePropInstance'
            }
        })
        .get('/api/rest_v1/page/summary/Main%20Page')
        .query({ redirect: false })
        .reply(200, { });

        return producer.sendAsync([{
            topic: 'test_dc.resource_change',
            messages: [
                JSON.stringify({
                    meta: {
                        topic: 'resource_change',
                        schema_uri: 'resource_change/1',
                        uri: 'https://en.wikipedia.org/api/rest_v1/page/html/Main%20Page',
                        request_id: common.SAMPLE_REQUEST_ID,
                        id: uuid.now(),
                        dt: new Date().toISOString(),
                        domain: 'en.wikipedia.org'
                    },
                    tags: ['restbase']
                })
            ]
        }])
        .delay(common.REQUEST_CHECK_DELAY)
        .then(() => mwAPI.done())
        .finally(() => nock.cleanAll());
    });

    it('Should update definition endpoint', () => {
        const mwAPI = nock('https://en.wiktionary.org', {
            reqheaders: {
                'cache-control': 'no-cache',
                'x-triggered-by': 'resource_change:https://en.wiktionary.org/api/rest_v1/page/html/Main%20Page',
                'x-request-id': common.SAMPLE_REQUEST_ID,
                'user-agent': 'SampleChangePropInstance'
            }
        })
        .get('/api/rest_v1/page/definition/Main%20Page')
        .query({ redirect: false })
        .reply(200, { });

        return producer.sendAsync([{
            topic: 'test_dc.resource_change',
            messages: [
                JSON.stringify({
                    meta: {
                        topic: 'resource_change',
                        schema_uri: 'resource_change/1',
                        uri: 'https://en.wiktionary.org/api/rest_v1/page/html/Main%20Page',
                        request_id: common.SAMPLE_REQUEST_ID,
                        id: uuid.now(),
                        dt: new Date().toISOString(),
                        domain: 'en.wiktionary.org'
                    },
                    tags: ['restbase']
                })
            ]
        }])
        .delay(common.REQUEST_CHECK_DELAY)
        .then(() => mwAPI.done())
        .finally(() => nock.cleanAll());
    });

    it('Should update mobile apps endpoint', () => {
        const mwAPI = nock('https://en.wikipedia.org', {
            reqheaders: {
                'cache-control': 'no-cache',
                'x-triggered-by': 'resource_change:https://en.wikipedia.org/api/rest_v1/page/html/Main%20Page',
                'x-request-id': common.SAMPLE_REQUEST_ID,
                'user-agent': 'SampleChangePropInstance'
            }
        })
        .get('/api/rest_v1/page/mobile-sections/Main%20Page')
        .query({ redirect: false })
        .reply(200, { });

        return producer.sendAsync([{
            topic: 'test_dc.resource_change',
            messages: [
                JSON.stringify({
                    meta: {
                        topic: 'resource_change',
                        schema_uri: 'resource_change/1',
                        uri: 'https://en.wikipedia.org/api/rest_v1/page/html/Main%20Page',
                        request_id: common.SAMPLE_REQUEST_ID,
                        id: uuid.now(),
                        dt: new Date().toISOString(),
                        domain: 'en.wikipedia.org'
                    },
                    tags: ['restbase']
                })
            ]
        }])
        .delay(common.REQUEST_CHECK_DELAY)
        .then(() => mwAPI.done())
        .finally(() => nock.cleanAll());
    });

    it('Should not update definition endpoint for non-main namespace', (done) => {
        const mwAPI = nock('https://en.wiktionary.org', {
            reqheaders: {
                'cache-control': 'no-cache',
                'x-request-id': common.SAMPLE_REQUEST_ID,
                'user-agent': 'SampleChangePropInstance'
            }
        })
        .get('/api/rest_v1/page/definition/User%3APchelolo')
        .reply(200, () => {
            done(new Error('Update was made while it should not have'))
        });

        return producer.sendAsync([{
            topic: 'test_dc.resource_change',
            messages: [
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
                })
            ]
        }])
        .delay(common.REQUEST_CHECK_DELAY)
        .finally(() => {
            nock.cleanAll();
            if (!mwAPI.isDone()) {
                done();
            }
        });
    });

    it('Should update RESTBase on resource_change from MW', () => {
        const mwAPI = nock('https://en.wikipedia.org', {
            reqheaders: {
                'cache-control': 'no-cache',
                'x-triggered-by': 'resource_change:https://en.wikipedia.org/wiki/Main_Page',
                'x-request-id': common.SAMPLE_REQUEST_ID,
                'if-unmodified-since': 'Thu, 01 Jan 1970 00:00:01 +0000',
                'user-agent': 'SampleChangePropInstance'
            }
        })
        .get('/api/rest_v1/page/html/Main_Page')
        .query({ redirect: false })
        .reply(200, { });

        return producer.sendAsync([{
            topic: 'test_dc.resource_change',
            messages: [
                JSON.stringify({
                    meta: {
                        topic: 'resource_change',
                        schema_uri: 'resource_change/1',
                        uri: 'https://en.wikipedia.org/wiki/Main_Page',
                        request_id: common.SAMPLE_REQUEST_ID,
                        id: uuid.now(),
                        dt: new Date(1).toISOString(),
                        domain: 'en.wikipedia.org'
                    },
                    tags: ['purge']
                })
            ]
        }])
        .delay(common.REQUEST_CHECK_DELAY)
        .then(() => mwAPI.done())
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

        return producer.sendAsync([{
            topic: 'test_dc.mediawiki.revision_create',
            messages: [
                JSON.stringify({
                    meta: {
                        topic: 'mediawiki.revision_create',
                        schema_uri: 'revision_create/1',
                        uri: '/edit/uri',
                        request_id: common.SAMPLE_REQUEST_ID,
                        id: uuid.now(),
                        dt: new Date(1).toISOString(),
                        domain: 'en.wikipedia.org'
                    },
                    page_title: 'User:Pchelolo/Test',
                    rev_id: 1234,
                    rev_timestamp: new Date().toISOString(),
                    rev_parent_id: 1233
                })
            ]
        }])
        .delay(common.REQUEST_CHECK_DELAY)
        .then(() => mwAPI.done())
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

        return producer.sendAsync([{
            topic: 'test_dc.mediawiki.page_delete',
            messages: [
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
                })
            ]
        }])
        .delay(common.REQUEST_CHECK_DELAY)
        .then(() => mwAPI.done())
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

        return producer.sendAsync([{
            topic: 'test_dc.mediawiki.page_restore',
            messages: [
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
                })
            ]
        }])
        .delay(common.REQUEST_CHECK_DELAY)
        .then(() => mwAPI.done())
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
        .get('/api/rest_v1/page/title/User%3APchelolo%2FTest')
        .query({ redirect: false })
        .reply(200, { })
        .get('/api/rest_v1/page/html/User%3APchelolo%2FTest1/2')
        .matchHeader( 'if-unmodified-since', 'Thu, 01 Jan 1970 00:00:01 +0000')
        .query({ redirect: false })
        .reply(200, { });

        return producer.sendAsync([{
            topic: 'test_dc.mediawiki.page_move',
            messages: [
                JSON.stringify({
                    meta: {
                        topic: 'mediawiki.page_move',
                        schema_uri: 'page_move/1',
                        uri: '/move/uri',
                        request_id: common.SAMPLE_REQUEST_ID,
                        id: uuid.now(),
                        dt: new Date(1).toISOString(),
                        domain: 'en.wikipedia.org'
                    },
                    old_title: 'User:Pchelolo/Test',
                    new_title: 'User:Pchelolo/Test1',
                    old_revision_id: 1,
                    new_revision_id: 2
                })
            ]
        }])
        .delay(common.REQUEST_CHECK_DELAY)
        .then(() => mwAPI.done())
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

        return producer.sendAsync([{
            topic: 'test_dc.mediawiki.revision_visibility_set',
            messages: [
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
                })
            ]
        }])
        .delay(common.REQUEST_CHECK_DELAY)
        .then(() => mwAPI.done())
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

        return producer.sendAsync([{
            topic: 'test_dc.resource_change',
            messages: [
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
                    tags: [ 'restbase' ]
                })
            ]
        }])
        .delay(common.REQUEST_CHECK_DELAY)
        .finally(() => {
            if (!closed) {
                udpServer.close();
                done(new Error('Timeout!'));
            }
        });
    });

    after(() => changeProp.stop());
});
