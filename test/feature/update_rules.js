"use strict";

const ChangeProp = require('../utils/changeProp');
const KafkaFactory = require('../../lib/kafka_factory');
const nock = require('nock');
const uuid = require('cassandra-uuid').TimeUuid;
const common = require('../utils/common');
const dgram  = require('dgram');
const assert = require('assert');

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
        this.timeout(0);

        return kafkaFactory.newProducer(kafkaFactory.newClient())
        .then((newProducer) => {
            producer = newProducer;
            return producer.createTopicsAsync(common.ALL_TOPICS, false)
        })
        .then(() => changeProp.start());
    });

    it('Should update summary endpoint', () => {
        const mwAPI = nock('https://en.wikipedia.org', {
            reqheaders: {
                'cache-control': 'no-cache'
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
                        request_id: uuid.now(),
                        id: uuid.now(),
                        dt: new Date().toISOString(),
                        domain: 'en.wikipedia.org'
                    },
                    tags: ['restbase']
                })
            ]
        }])
        .delay(300)
        .then(() => mwAPI.done())
        .finally(() => nock.cleanAll());
    });

    it('Should update definition endpoint', () => {
        const mwAPI = nock('https://en.wiktionary.org', {
            reqheaders: {
                'cache-control': 'no-cache'
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
                        request_id: uuid.now(),
                        id: uuid.now(),
                        dt: new Date().toISOString(),
                        domain: 'en.wiktionary.org'
                    },
                    tags: ['restbase']
                })
            ]
        }])
        .delay(300)
        .then(() => mwAPI.done())
        .finally(() => nock.cleanAll());
    });

    it('Should update mobile apps endpoint', () => {
        const mwAPI = nock('https://en.wikipedia.org', {
            reqheaders: {
                'cache-control': 'no-cache'
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
                        request_id: uuid.now(),
                        id: uuid.now(),
                        dt: new Date().toISOString(),
                        domain: 'en.wikipedia.org'
                    },
                    tags: ['restbase']
                })
            ]
        }])
        .delay(300)
        .then(() => mwAPI.done())
        .finally(() => nock.cleanAll());
    });

    it('Should not update definition endpoint for non-main namespace', (done) => {
        const mwAPI = nock('https://en.wiktionary.org', {
            reqheaders: {
                'cache-control': 'no-cache'
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
                        request_id: uuid.now(),
                        id: uuid.now(),
                        dt: new Date().toISOString(),
                        domain: 'en.wiktionary.org'
                    },
                    tags: ['restbase']
                })
            ]
        }])
        .delay(300)
        .finally(() => {
            nock.cleanAll();
            if (!mwAPI.isDone()) {
                done();
            }
        });
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
        .delay(300)
        .finally(() => {
            if (!closed) {
                udpServer.close();
                done(new Error('Timeout!'));
            }
        });
    });

    after(() => changeProp.stop());
});
