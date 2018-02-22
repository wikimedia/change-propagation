"use strict";

const uuid         = require('cassandra-uuid').TimeUuid;
const P            = require('bluebird');
const KafkaFactory = require('../../lib/kafka_factory');
const assert       = require('assert');

const common = {};

common.topics_created = false;
common.REQUEST_CHECK_DELAY = 3000;

common.SAMPLE_REQUEST_ID = uuid.now().toString();

common.eventWithProperties = (topic, props) => {
    const event = {
        meta: {
            topic: topic,
            schema_uri: 'schema/1',
            uri: '/sample/uri',
            request_id: common.SAMPLE_REQUEST_ID,
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

common.eventWithMessageAndRandom = (message, random) => {
    return common.eventWithProperties('simple_test_rule', {
        message: message,
        random: random
    });
};

common.randomString = (len = 5) => {
    const possible = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
    let text = '';
    while (text.length < len) {
        text += possible.charAt(Math.floor(Math.random() * possible.length));
    }
    return text;
};

common.arrayWithLinks = function(link, num) {
    const result = [];
    for(let idx = 0; idx < num; idx++) {
        result.push({
            pageid: 1,
            ns: 0,
            title: link
        });
    }
    return result;
};

common.EN_SITE_INFO_RESPONSE = {
    "query": {
        "general": {
            "legaltitlechars": " %!\"$&'()*,\\-.\\/0-9:;=?@A-Z\\\\^_`a-z~\\x80-\\xFF+",
            "case": "first-letter",
            "lang": "en"
        },
        "namespaces": {
            "-2": {"id": -2, "case": "first-letter", "canonical": "Media", "*": "Media"},
            "-1": {"id": -1, "case": "first-letter", "canonical": "Special", "*": "Special"},
            "0": {"id": 0, "case": "first-letter", "content": "", "*": ""},
            "1": {"id": 1, "case": "first-letter", "subpages": "", "canonical": "Talk", "*": "Talk"},
            "2": {"id": 2, "case": "first-letter", "subpages": "", "canonical": "User", "*": "User"},
            "3": {"id": 3, "case": "first-letter", "subpages": "", "canonical": "User talk", "*": "User talk"},
            "4": {"id": 4, "case": "first-letter", "subpages": "", "canonical": "Project", "*": "Wikipedia"},
            "5": {"id": 5, "case": "first-letter", "subpages": "", "canonical": "Project talk", "*": "Wikipedia talk"},
            "6": {"id": 6, "case": "first-letter", "canonical": "File", "*": "File"},
            "7": {"id": 7, "case": "first-letter", "subpages": "", "canonical": "File talk", "*": "File talk"}
        }
    }
};

common.checkAPIDone = (api, maxAttempts) => {
    maxAttempts = maxAttempts || 50;
    let attempts = 0;
    const check = () => {
        if (api.isDone()) {
            return;
        } else if (attempts++ < maxAttempts) {
            return P.delay(500).then(check);
        } else {
            return api.done();
        }
    };
    return check();
};

common.checkPendingMocks = (api, num) => {
    return P.delay(2000).then(() =>  assert.equal(api.pendingMocks().length, num));
};

common.factory = new KafkaFactory({
    metadata_broker_list: '127.0.0.1:9092',
    producer: {
        'queue.buffering.max.ms': '1'
    },
    consumer: {
        default_topic_conf: {
            "auto.offset.reset": "largest"
        },
        "group.id": 'change-prop-test-consumer',
        "fetch.wait.max.ms": "1",
        "fetch.min.bytes": "1",
        "queue.buffering.max.ms": "1"
    },
    log: console.log.bind(console)
});


// Sample JobQueue events

common.jobs = {
    get updateBetaFeaturesUserCounts() {
        return {
            "database": "enwiki",
            "meta": {
                "domain": "en.wikipedia.org",
                "dt": new Date().toISOString(),
                "id": uuid.now().toString(),
                "request_id": common.randomString(10),
                "schema_uri": "mediawiki/job/1",
                "topic": "mediawiki.job.updateBetaFeaturesUserCounts",
                "uri": "https://en.wikipedia.org/wiki/Main_Page"
            },
            "page_namespace": 0,
            "page_title": "Main_Page",
            "params": {
                "prefs": [
                    "visualeditor-newwikitext"
                ],
                "requestId": "Wa4HNApAAEMAAHzG8r4AAAAE"
            },
            "sha1": common.randomString(20),
            "type": "updateBetaFeaturesUserCounts"
        };
    },
    get htmlCacheUpdate() {
        const rootSignature = common.randomString(10);
        return {
            "database": "commonswiki",
            "mediawiki_signature": common.randomString(10),
            "meta": {
                "domain": "commons.wikimedia.org",
                "dt": new Date().toISOString(),
                "id": uuid.now().toString(),
                "request_id": common.randomString(10),
                "schema_uri": "mediawiki/job/1",
                "topic": "mediawiki.job.htmlCacheUpdate",
                "uri": "https://commons.wikimedia.org/wiki/File:%D0%A1%D1%82%D0%B0%D0%B2%D0%BE%D0%BA_-_panoramio_(6).jpg"
            },
            "page_namespace": 6,
            "page_title": "File:Ставок_-_panoramio_(6).jpg",
            "params": {
                "causeAction": "page-edit",
                "causeAgent": "unknown",
                "recursive": true,
                "requestId": "Wi7xIQpAANEAAEs6jxcAAACE",
                "rootJobIsSelf": true,
                "rootJobSignature": rootSignature,
                "rootJobTimestamp": "20171211205706",
                "table": "redirect"
            },
            "root_event": {
                "dt": new Date().toISOString(),
                "signature": rootSignature
            },
            "sha1": common.randomString(10),
            "type": "htmlCacheUpdate"
        };
    }
};

module.exports = common;
