"use strict";

const ChangeProp = require('../utils/changeProp');
const KafkaFactory = require('../../lib/kafka_factory');
const nock = require('nock');
const uuid = require('cassandra-uuid').TimeUuid;

describe('RESTBase update rules', function() {
    this.timeout(1000);

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
            return producer.createTopicsAsync([
                'test_dc.resource_change',
                'test_dc.change-prop.retry.resource_change'
            ], false)
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
    
    after(() => changeProp.stop());
});
