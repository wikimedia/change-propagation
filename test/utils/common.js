"use strict";

const uuid = require('cassandra-uuid').TimeUuid;

const common = {};

common.topics_created = false;
common.REQUEST_CHECK_DELAY = 300;

common.ALL_TOPICS = [
    'test_dc.simple_test_rule',
    'test_dc.change-prop.retry.simple_test_rule',
    'test_dc.kafka_producing_rule',
    'test_dc.change-prop.retry.kafka_producing_rule',
    'test_dc.mediawiki.revision_create',
    'test_dc.change-prop.retry.mediawiki.revision_create',
    'test_dc.change-prop.backlinks.continue',
    'test_dc.change-prop.retry.change-prop.backlinks.continue',
    'test_dc.resource_change',
    'test_dc.change-prop.retry.resource_change'
];

common.eventWithProperties = (topic, props) => {
    const event = {
        meta: {
            topic: topic,
            schema_uri: 'schema/1',
            uri: '/sample/uri',
            request_id: uuid.now(),
            id: uuid.now(),
            dt: new Date().toISOString(),
            domain: 'en.wikipedia.org'
        }
    };
    Object.assign(event, props);
    return event;
};

common.eventWithMessage = (message) => {
    return common.eventWithProperties('simple_test_rule', { message: message });
};

module.exports = common;