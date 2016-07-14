"use strict";

const ChangeProp = require('../utils/changeProp');
const KafkaFactory = require('../../lib/kafka_factory');
const nock = require('nock');
const common = require('../utils/common');
const P = require('bluebird');

describe('Startup', function () {
    this.timeout(10000);

    const changeProp = new ChangeProp('config.test.yaml');
    const kafkaFactory = new KafkaFactory({
        uri: 'localhost:2181/', // TODO: find out from the config
        clientId: 'change-prop-test-suite',
        dc_name: 'test_dc'
    });
    let producer;

    before(() => {
        return kafkaFactory.newProducer()
        .then((newProducer) => {
            producer = newProducer;
            if (!common.topics_created) {
                common.topics_created = true;
                return P.each(common.ALL_TOPICS, (topic) => {
                    return producer.createTopicsAsync([ topic ], false);
                });
            }
            return P.resolve();
        });
    });

    it('Should start from latest offset if new rule is created', (done) => {
        let finished = false;
        nock('http://mock.com').post('/').reply(() => P.try(() => {
            if (!finished) {
                finished = true;
                changeProp.stop().then(() =>
                    done(new Error('The event must not have been processed')))
            }
        }));

        // Produce a message to a test topic. As the topics were deleted before,
        // the offset should not exist in zookeeper, so the first test will verify that
        // we are not going through the backlog when adding new consumer groups
        return producer.sendAsync([{
            topic: 'test_dc.simple_test_rule',
            messages: [
                JSON.stringify(common.eventWithMessage('test'))
            ]
        }])
        .delay(100)
        .then(() => changeProp.start())
        .delay(1000)
        .finally(() => {
            if (!finished) {
                changeProp.stop().then(() => done());
            }
            nock.cleanAll()
        });
    });
});