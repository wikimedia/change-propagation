"use strict";

const ChangeProp = require('../utils/changeProp');
const KafkaFactory = require('../../lib/kafka_factory');
const nock = require('nock');
const preq = require('preq');
const Ajv = require('ajv');
const assert = require('assert');
const yaml = require('js-yaml');
const common = require('../utils/common');
const P = require('bluebird');
const kafka = require('librdkafka-node');

describe('Basic rule management', function() {
    this.timeout(10000);

    const changeProp = new ChangeProp('config.test.yaml');
    const kafkaFactory = new KafkaFactory({
        uri: 'localhost:2181/', // TODO: find out from the config
        clientId: 'change-prop-test-suite',
        dc_name: 'test_dc'
    });
    let producer = new kafka.Producer({
        "metadata.broker.list": "127.0.0.1:9092",
        "queue.buffering.max.ms": "1"
    });
    let retrySchema;
    let errorSchema;
    let siteInfoResponse;

    before(function() {
        // Setting up might tike some tome, so disable the timeout
        this.timeout(20000);

        return changeProp.start()
        .then(() => preq.get({
                uri: 'https://raw.githubusercontent.com/wikimedia/mediawiki-event-schemas/master/jsonschema/change-prop/retry/1.yaml'
        }))
        .then((res) => retrySchema = yaml.safeLoad(res.body))
        .then(() => preq.get({
                uri: 'https://raw.githubusercontent.com/wikimedia/mediawiki-event-schemas/master/jsonschema/error/1.yaml'
        }))
        .then((res) => errorSchema = yaml.safeLoad(res.body))
        .then(() => {
            preq.post({
                uri: 'https://en.wikipedia.org/w/api.php',
                body: {
                    format: 'json',
                    action: 'query',
                    meta: 'siteinfo',
                    siprop: 'general|namespaces|namespacealiases'
                }
            });
        })
        .then((res) => siteInfoResponse = res)
    });

    it('Should call simple executor', () => {
        const random = common.randomString();
        const service = nock('http://mock.com', {
            reqheaders: {
                test_header_name: 'test_header_value',
                'content-type': 'application/json',
                'x-request-id': common.SAMPLE_REQUEST_ID,
                'x-triggered-by': 'simple_test_rule:/sample/uri',
                'user-agent': 'ChangePropTestSuite'
            }
        })
        .post('/', {
            'test_field_name': 'test_field_value',
            'derived_field': 'test',
            'random_field': random
        }).reply({});

        return P.each([
            JSON.stringify(common.eventWithMessageAndRandom('this_will_not_match', random)),
            JSON.stringify(common.eventWithMessageAndRandom('test', random)),
            // The empty message should cause a failure in the match test
            '{}'
        ], (msg) => {
            return producer.produce(`test_dc.simple_test_rule`, msg);
        })
        .delay(common.REQUEST_CHECK_DELAY)
        .then(() => service.done())
        .finally(() => nock.cleanAll());
    });

    it('Should retry simple executor', () => {
        const random = common.randomString();
        const service = nock('http://mock.com', {
            reqheaders: {
                test_header_name: 'test_header_value',
                'content-type': 'application/json',
                'x-request-id': common.SAMPLE_REQUEST_ID,
                'user-agent': 'ChangePropTestSuite'
            }
        })
        .post('/', {
            'test_field_name': 'test_field_value',
            'derived_field': 'test',
            'random_field': random
        })
        .matchHeader('x-triggered-by', 'simple_test_rule:/sample/uri')
        .reply(500, {})
        .post('/', {
            'test_field_name': 'test_field_value',
            'derived_field': 'test',
            'random_field': random
        })
        .matchHeader('x-triggered-by', 'simple_test_rule:/sample/uri,change-prop.retry.simple_test_rule:/sample/uri')
        .reply(200, {});

        return producer.produce('test_dc.simple_test_rule',
            JSON.stringify(common.eventWithMessageAndRandom('test', random)))
        .delay(common.REQUEST_CHECK_DELAY)
        .then(() => service.done())
        .finally(() => nock.cleanAll());
    });

    it('Should retry simple executor no more than limit', () => {
        const random = common.randomString();
        const service = nock('http://mock.com', {
            reqheaders: {
                test_header_name: 'test_header_value',
                'content-type': 'application/json',
                'x-request-id': common.SAMPLE_REQUEST_ID,
                'user-agent': 'ChangePropTestSuite'
            }
        })
        .post('/', {
            'test_field_name': 'test_field_value',
            'derived_field': 'test',
            'random_field': random
        })
        .matchHeader('x-triggered-by', 'simple_test_rule:/sample/uri')
        .reply(500, {})
        .post('/', {
            'test_field_name': 'test_field_value',
            'derived_field': 'test',
            'random_field': random
        })
        .matchHeader('x-triggered-by', 'simple_test_rule:/sample/uri,change-prop.retry.simple_test_rule:/sample/uri')
        .reply(500, {})
        .post('/', {
            'test_field_name': 'test_field_value',
            'derived_field': 'test',
            'random_field': random
        })
        .matchHeader('x-triggered-by', 'simple_test_rule:/sample/uri,change-prop.retry.simple_test_rule:/sample/uri,change-prop.retry.simple_test_rule:/sample/uri')
        .reply(500, {})
        // Next one must never get called, we verify that by checking pending mocks
        .post('/', {
            'test_field_name': 'test_field_value',
            'derived_field': 'test',
            'random_field': random
        })
        .reply(500, {});

        return producer.produce('test_dc.simple_test_rule',
            JSON.stringify(common.eventWithMessageAndRandom('test', random)))
        .delay(common.REQUEST_CHECK_DELAY)
        .then(() => {
            assert.equal(service.pendingMocks().length, 1);
        })
        .finally(() => nock.cleanAll());
    });

    it('Should emit valid retry message', function() {
        this.timeout(10000);
        const random = common.randomString();
        nock('http://mock.com', {
            reqheaders: {
                test_header_name: 'test_header_value',
                'content-type': 'application/json',
                'x-request-id': common.SAMPLE_REQUEST_ID,
                'user-agent': 'ChangePropTestSuite'
            }
        })
        .post('/', {
            'test_field_name': 'test_field_value',
            'derived_field': 'test',
            'random_field': random
        })
        .reply(500, {})
        .post('/', {
            'test_field_name': 'test_field_value',
            'derived_field': 'test',
            'random_field': random
        })
        .reply(200, {});

        const retryConsumer = new kafka.KafkaConsumer({
            "default_topic_conf": {
                "auto.offset.reset": "largest"
            },
            "group.id": 'change-prop-test-consumer-valid-retry',
            "metadata.broker.list": "127.0.0.1:9092",
            "fetch.wait.max.ms": "1",
            "fetch.min.bytes": "1",
            "queue.buffering.max.ms": "1"
        });
        retryConsumer.subscribe([ 'change-prop.retry.simple_test_rule' ]);
        setTimeout(() => producer.produce('test_dc.simple_test_rule',
            JSON.stringify(common.eventWithMessageAndRandom('test', random))), 4000);
        return retryConsumer.consume()
        .then((message) => {
            console.log(message);
            const ajv = new Ajv();
            const validate = ajv.compile(retrySchema);
            const msg = JSON.parse(message.payload.toString());
            const valid = validate(msg);
            if (!valid) {
                throw new assert.AssertionError({
                    message: ajv.errorsText(validate.errors)
                });
            } else if (msg.triggered_by !== 'simple_test_rule:/sample/uri') {
                throw new Error('TriggeredBy should be equal to simple_test_rule:/sample/uri');
            }
        });
    });

    it('Should not retry if retry_on not matched', () => {
        const random = common.randomString();
        const service = nock('http://mock.com', {
            reqheaders: {
                test_header_name: 'test_header_value',
                'content-type': 'application/json',
                'x-request-id': common.SAMPLE_REQUEST_ID,
                'x-triggered-by': 'simple_test_rule:/sample/uri',
                'user-agent': 'ChangePropTestSuite'
            }
        })
        .post('/', {
            'test_field_name': 'test_field_value',
            'derived_field': 'test',
            'random_field': random
        })
        .reply(404, {})
        // Next one must never get called, we verify that by checking pending mocks
        .post('/', {
            'test_field_name': 'test_field_value',
            'derived_field': 'test',
            'random_field': random
        })
        .reply(404, {});

        return producer.produce('test_dc.simple_test_rule',
            JSON.stringify(common.eventWithMessageAndRandom('test', random)))
        .delay(common.REQUEST_CHECK_DELAY)
        .then(() => assert.equal(service.pendingMocks().length, 1))
        .finally(() => nock.cleanAll());
    });

    it('Should not follow redirects', () => {
        const service = nock('http://mock.com/')
        .get('/will_redirect')
        .reply(301, '', {
            'location': 'http://mock.com/redirected_resource'
        })
        // Next one must never get called, we verify that by checking pending mocks
        .get('/redirected_resource')
        .reply(200, {});

        return producer.produce('test_dc.simple_test_rule',
            JSON.stringify(common.eventWithMessage('redirect')))
        .delay(common.REQUEST_CHECK_DELAY)
        .then(() => assert.equal(service.pendingMocks().length, 1))
        .finally(() => nock.cleanAll());
    });

    it('Should not crash with unparsable JSON', () => {
        const service = nock('http://mock.com', {
            reqheaders: {
                test_header_name: 'test_header_value',
                'content-type': 'application/json',
                'x-request-id': common.SAMPLE_REQUEST_ID,
                'x-triggered-by': 'simple_test_rule:/sample/uri',
                'user-agent': 'ChangePropTestSuite'
            }
        })
        .post('/', {
            'test_field_name': 'test_field_value',
            'derived_field': 'test'
        })
        .reply(200, {});

        return P.each([
            'non-parsable-json',
            JSON.stringify(common.eventWithMessage('test'))
        ], (msg) => producer.produce('test_dc.simple_test_rule', msg))
        .delay(common.REQUEST_CHECK_DELAY)
        .then(() => service.done())
        .finally(() => nock.cleanAll());
    });

    it('Should support producing to topics on exec', () => {
        const service = nock('http://mock.com', {
            reqheaders: {
                test_header_name: 'test_header_value',
                'content-type': 'application/json',
                'x-request-id': common.SAMPLE_REQUEST_ID,
                'user-agent': 'ChangePropTestSuite'
            }
        })
        .post('/', {
            'test_field_name': 'test_field_value',
            'derived_field': 'test'
        })
        .matchHeader('x-triggered-by', 'test_dc.kafka_producing_rule:/sample/uri,simple_test_rule:/sample/uri')
        .times(2).reply({});

        return producer.produce('test_dc.kafka_producing_rule',
            JSON.stringify(common.eventWithProperties('test_dc.kafka_producing_rule', {
                produce_to_topic: 'simple_test_rule'
            })))
        .delay(common.REQUEST_CHECK_DELAY)
        .then(() => service.done())
        .finally(() => nock.cleanAll());
    });

    it('Should process backlinks', () => {
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
            list: 'backlinks',
            bltitle: 'Main_Page',
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
        .matchHeader('x-triggered-by', 'mediawiki.revision_create:/sample/uri,resource_change:https://en.wikipedia.org/wiki/Some_Page')
        .times(2)
        .reply(200)
        .post('/w/api.php', {
            format: 'json',
            action: 'query',
            list: 'backlinks',
            bltitle: 'Main_Page',
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
        .matchHeader('x-triggered-by', 'mediawiki.revision_create:/sample/uri,resource_change:https://en.wikipedia.org/wiki/Some_Page')
        .reply(200);

        return producer.produce('test_dc.mediawiki.revision_create',
            JSON.stringify(common.eventWithProperties('mediawiki.revision_create', { title: 'Main_Page' })))
        .delay(common.REQUEST_CHECK_DELAY)
        .then(() => mwAPI.done())
        .finally(() => nock.cleanAll());
    });

    it('Should emit valid messages to error topic', () => {
        const errorConsumer = new kafka.KafkaConsumer({
            "default_topic_conf": {
                "auto.offset.reset": "smallest"
            },
            "group.id": 'change-prop-test-error-consumer',
            "metadata.broker.list": "127.0.0.1:9092",
            "fetch.wait.max.ms": "1",
            "fetch.min.bytes": "1",
            "queue.buffering.max.ms": "1"
        });
        errorConsumer.subscribe([ 'test_dc.change-prop.error' ]);
        setTimeout(() => producer.produce('test_dc.mediawiki.revision_create', 'not_a_json_message'), 1000);
        return errorConsumer.consume()
        .then((message) => {
            const ajv = new Ajv();
            const validate = ajv.compile(errorSchema);
            var valid = validate(JSON.parse(message.payload.toString()));
            if (!valid) {
                throw  new assert.AssertionError({
                    message: ajv.errorsText(validate.errors)
                });
            }
        });
    });

    after(() => changeProp.stop());
});