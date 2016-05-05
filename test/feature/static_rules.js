"use strict";

const ChangeProp = require('../utils/changeProp');
const KafkaFactory = require('../../lib/kafka_factory');
const nock = require('nock');
const preq = require('preq');
const Ajv = require('ajv');
const assert = require('assert');
const yaml = require('js-yaml');
const common = require('../utils/common');

describe('Basic rule management', function() {
    this.timeout(2000);

    const changeProp = new ChangeProp('config.test.yaml');
    const kafkaFactory = new KafkaFactory({
        uri: 'localhost:2181/', // TODO: find out from the config
        clientId: 'change-prop-test-suite',
        dc_name: 'test_dc'
    });
    let producer;
    let retrySchema;

    before(function() {
        // Setting up might tike some tome, so disable the timeout
        this.timeout(0);

        return kafkaFactory.newProducer(kafkaFactory.newClient())
        .then((newProducer) => {
            producer = newProducer;
            return producer.createTopicsAsync(common.ALL_TOPICS, false)
        })
        .then(() => changeProp.start())
        .then(() => preq.get({
                uri: 'https://raw.githubusercontent.com/wikimedia/mediawiki-event-schemas/master/jsonschema/retry/1.yaml'
        }))
        .then((res) => retrySchema = yaml.safeLoad(res.body));
    });

    function arrayWithLinks(link, num) {
        const result = [];
        for(let idx = 0; idx < num; idx++) {
            result.push({
                pageid: 1,
                ns: 0,
                title: link
            });
        }
        return result;
    }

    it('Should process backlinks', () => {
        const mwAPI = nock('https://en.wikipedia.org')
        .post('/w/api.php', {
            format: 'json',
            action: 'query',
            list: 'backlinks',
            bltitle: 'Main_Page',
            blfilterredir: 'nonredirects',
            bllimit: '500' })
        .reply(200, {
            batchcomplete: '',
            continue: {
                blcontinue: '1|2272',
                continue: '-||'
            },
            query: {
                backlinks: arrayWithLinks('Some_Page', 2)
            }
        })
        .get('/api/rest_v1/page/html/Some_Page').times(2).reply(200)
        .post('/w/api.php', {
            format: 'json',
            action: 'query',
            list: 'backlinks',
            bltitle: 'Main_Page',
            blfilterredir: 'nonredirects',
            bllimit: '500',
            blcontinue: '1|2272'
        })
        .reply(200, {
            batchcomplete: '',
            query: {
                backlinks: arrayWithLinks('Some_Page', 1)
            }
        })
        .get('/api/rest_v1/page/html/Some_Page').reply(200);
        return producer.sendAsync([{
            topic: 'test_dc.mediawiki.revision_create',
            messages: [
                JSON.stringify(common.eventWithProperties('mediawiki.revision_create', { title: 'Main_Page' }))
            ]
        }])
        .delay(300)
        .then(() => mwAPI.done())
        .finally(() => nock.cleanAll());
    });

    it('Should call simple executor', () => {
        const service = nock('http://mock.com', {
            reqheaders: {
                test_header_name: 'test_header_value',
                'content-type': 'application/json'
            }
        })
        .post('/', {
            'test_field_name': 'test_field_value',
            'derived_field': 'test'
        }).reply({});

        return producer.sendAsync([{
            topic: 'test_dc.simple_test_rule',
            messages: [
                JSON.stringify(common.eventWithMessage('this_will_not_match')),
                JSON.stringify(common.eventWithMessage('test')) ]
        }])
        .delay(100)
        .then(() => service.done())
        .finally(() => nock.cleanAll());
    });

    it('Should retry simple executor', () => {
        const service = nock('http://mock.com', {
            reqheaders: {
                test_header_name: 'test_header_value',
                'content-type': 'application/json'
            }
        })
        .post('/', {
            'test_field_name': 'test_field_value',
            'derived_field': 'test'
        }).reply(500, {})
        .post('/', {
            'test_field_name': 'test_field_value',
            'derived_field': 'test'
        }).reply(200, {});

        return producer.sendAsync([{
            topic: 'test_dc.simple_test_rule',
            messages: [ JSON.stringify(common.eventWithMessage('test')) ]
        }])
        .delay(300)
        .then(() => service.done())
        .finally(() => nock.cleanAll());
    });

    it('Should emit valid retry message', (done) => {
        // No need to emit new messages, we will use on from previous test
        return kafkaFactory.newConsumer(kafkaFactory.newClient(),
            'change-prop.retry.simple_test_rule',
            'change-prop-test-consumer')
        .then((retryConsumer) => {
            retryConsumer.once('message', (message) => {
                try {
                    const ajv = new Ajv();
                    const validate = ajv.compile(retrySchema);
                    var valid = validate(JSON.parse(message.value));
                    if (!valid) {
                        done(new assert.AssertionError({
                            message: ajv.errorsText(validate.errors)
                        }));
                    } else {
                        done();
                    }
                } catch(e) {
                    done(e);
                }
            });
        });
    });

    it('Should retry simple executor no more than limit', () => {
        const service = nock('http://mock.com', {
            reqheaders: {
                test_header_name: 'test_header_value',
                'content-type': 'application/json'
            }
        })
        .post('/', {
            'test_field_name': 'test_field_value',
            'derived_field': 'test'
        }).times(3).reply(500, {});

        return producer.sendAsync([{
            topic: 'test_dc.simple_test_rule',
            messages: [ JSON.stringify(common.eventWithMessage('test')) ]
        }])
        .delay(300)
        .then(() => service.done())
        .finally(() => nock.cleanAll());
    });

    it('Should not retry if retry_on not matched', () => {
        const service = nock('http://mock.com', {
            reqheaders: {
                test_header_name: 'test_header_value',
                'content-type': 'application/json'
            }
        })
        .post('/', {
            'test_field_name': 'test_field_value',
            'derived_field': 'test'
        }).reply(404, {});

        return producer.sendAsync([{
            topic: 'test_dc.simple_test_rule',
            messages: [ JSON.stringify(common.eventWithMessage('test')) ]
        }])
        .delay(300)
        .then(() => service.done())
        .finally(() => nock.cleanAll());
    });

    it('Should not crash with unparsable JSON', () => {
        const service = nock('http://mock.com', {
            reqheaders: {
                test_header_name: 'test_header_value',
                'content-type': 'application/json'
            }
        })
        .post('/', {
            'test_field_name': 'test_field_value',
            'derived_field': 'test'
        }).reply(200, {});

        return producer.sendAsync([{
            topic: 'test_dc.simple_test_rule',
            messages: [ 'non-parsable-json', JSON.stringify(common.eventWithMessage('test')) ]
        }])
        .delay(100)
        .then(() => service.done())
        .finally(() => nock.cleanAll());
    });

    it('Should support producing to topics on exec', () => {
        const service = nock('http://mock.com', {
            reqheaders: {
                test_header_name: 'test_header_value',
                'content-type': 'application/json'
            }
        })
        .post('/', {
            'test_field_name': 'test_field_value',
            'derived_field': 'test'
        }).times(2).reply({});

        return producer.sendAsync([{
            topic: 'test_dc.kafka_producing_rule',
            messages: [ JSON.stringify(common.eventWithProperties('test_dc.kafka_producing_rule', {
                produce_to_topic: 'simple_test_rule'
            })) ]
        }])
        .delay(100)
        .then(() => service.done())
        .finally(() => nock.cleanAll());
    });

    after(() => changeProp.stop());
});