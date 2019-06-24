'use strict';

const uuidv1       = require('uuid/v1');
const P            = require('bluebird');
const kafkaFactory = require('../../lib/kafka_factory');
const MockFactory  = require('./mock_kafka_factory');
const assert       = require('assert');
const preq         = require('preq');
const yaml         = require('js-yaml');
const Ajv          = require('ajv');
const mockRequire  = require('mock-require');

const common = {};

common.topics_created = false;
common.REQUEST_CHECK_DELAY = 3000;

common.SAMPLE_REQUEST_ID = uuidv1();

common.eventWithProperties = (topic, props) => {
    const event = {
        meta: {
            topic,
            schema_uri: 'schema/1',
            uri: 'https://en.wikipedia.org/wiki/SamplePage',
            request_id: common.SAMPLE_REQUEST_ID,
            id: uuidv1(),
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

common.eventWithTopic = (topic) => {
    return common.eventWithProperties(topic, {});
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

common.arrayWithLinks = function (link, num) {
    const result = [];
    for (let idx = 0; idx < num; idx++) {
        result.push({
            pageid: 1,
            ns: 0,
            title: link
        });
    }
    return result;
};

common.EN_SITE_INFO_RESPONSE = {
    query: {
        general: {
            legaltitlechars: " %!\"$&'()*,\\-.\\/0-9:;=?@A-Z\\\\^_`a-z~\\x80-\\xFF+",
            case: 'first-letter',
            lang: 'en'
        },
        namespaces: {
            '-2': { id: -2, case: 'first-letter', canonical: 'Media', '*': 'Media' },
            '-1': { id: -1, case: 'first-letter', canonical: 'Special', '*': 'Special' },
            0: { id: 0, case: 'first-letter', content: '', '*': '' },
            1: { id: 1, case: 'first-letter', subpages: '', canonical: 'Talk', '*': 'Talk' },
            2: { id: 2, case: 'first-letter', subpages: '', canonical: 'User', '*': 'User' },
            3: { id: 3, case: 'first-letter', subpages: '', canonical: 'User talk', '*': 'User talk' },
            4: { id: 4, case: 'first-letter', subpages: '', canonical: 'Project', '*': 'Wikipedia' },
            5: { id: 5, case: 'first-letter', subpages: '', canonical: 'Project talk', '*': 'Wikipedia talk' },
            6: { id: 6, case: 'first-letter', canonical: 'File', '*': 'File' },
            7: { id: 7, case: 'first-letter', subpages: '', canonical: 'File talk', '*': 'File talk' }
        }
    }
};

// Most test timeouts are set to 10 sec or longer. By setting the maxAttempts value to 19,
// the retries will fail prior to the test timeout and show the correct failure message.
common.checkAPIDone = (api, maxAttempts = 19) => {
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

const validatorCache = new Map();
const ajv = new Ajv({
    schemaId: 'auto',
    loadSchema: (uri) => preq.get({ uri })
    .then((content) => {
        if (content.status !== 200) {
            throw new Error(`Failed to load meta schema at ${uri}`);
        }
        ajv.addMetaSchema(JSON.parse(content.body), uri);
    })
});

common.fetchEventValidator = (schemaUri, version = 1) => {
    const schemaPath = `${schemaUri}/${version}.yaml`;
    if (validatorCache.has(schemaPath)) {
        return P.resolve(validatorCache.get(schemaPath));
    }
    return preq.get({
        uri: `https://raw.githubusercontent.com/wikimedia/mediawiki-event-schemas/master/jsonschema/${schemaPath}`
    })
    .then((res) => ajv.compileAsync(yaml.safeLoad(res.body)))
    .then((validator) => {
        validatorCache.set(schemaPath, validator);
        return validator;
    });
};

common.getKafkaFactory = kafkaFactory.getFactory;
if (process.env.MOCK_SERVICES) {
    const mockKafkaFactory = new MockFactory();
    kafkaFactory.setFactory(mockKafkaFactory);
    common.clearKafkaFactory = () => {};
    mockRequire('redis', require('redis-mock'));
} else {
    common.clearKafkaFactory = () => {
        kafkaFactory.setFactory(undefined);
    };
}

// Sample ChangeProp events

const eventMethods = {
    toBuffer() {
        return Buffer.from(JSON.stringify(this));
    }
};

common.events = {
    resourceChange(
        uri = 'https://en.wikipedia.org/api/rest_v1/page/html/Main_Page',
        dt = new Date().toISOString(),
        tags = [ 'restbase' ]
    ) {
        const domain = /https?:\/\/([^/]+).+/.exec(uri)[1];
        return {
            __proto__: eventMethods,
            meta: {
                stream: 'resource_change',
                schema_uri: 'resource_change/1',
                uri,
                request_id: common.SAMPLE_REQUEST_ID,
                id: uuidv1(),
                dt,
                domain
            },
            tags
        };
    },
    revisionCreate(
        uri = 'https://en.wikipedia.org/api/rest_v1/page/html/Main_Page',
        dt = new Date().toISOString()
    ) {
        const domain = /https?:\/\/([^/]+).+/.exec(uri)[1];
        const title = /\/([^/]+)$/.exec(uri)[1];
        return {
            __proto__: eventMethods,
            meta: {
                stream: 'mediawiki.revision-create',
                schema_uri: 'revision-create/1',
                uri,
                request_id: common.SAMPLE_REQUEST_ID,
                id: uuidv1(),
                dt,
                domain
            },
            database: 'enwiki',
            page_title: title,
            page_id: 12345,
            page_namespace: 0,
            rev_id: 1234,
            rev_timestamp: new Date().toISOString(),
            rev_parent_id: 1233,
            performer: {
                user_text: 'I am a user',
                user_groups: [ 'I am a group' ],
                user_is_bot: false
            }
        };
    }
};

// Sample JobQueue events

common.jobs = {
    get updateBetaFeaturesUserCounts() {
        return {
            __proto__: eventMethods,
            database: 'enwiki',
            meta: {
                domain: 'en.wikipedia.org',
                dt: new Date().toISOString(),
                id: uuidv1(),
                request_id: common.randomString(10),
                schema_uri: 'mediawiki/job/1',
                topic: 'mediawiki.job.updateBetaFeaturesUserCounts',
                uri: 'https://en.wikipedia.org/wiki/Main_Page'
            },
            params: {
                prefs: [
                    'visualeditor-newwikitext'
                ],
                requestId: 'Wa4HNApAAEMAAHzG8r4AAAAE'
            },
            sha1: common.randomString(20),
            type: 'updateBetaFeaturesUserCounts'
        };
    },
    get htmlCacheUpdate() {
        const rootSignature = common.randomString(10);
        return {
            __proto__: eventMethods,
            database: 'commonswiki',
            mediawiki_signature: common.randomString(10),
            meta: {
                domain: 'commons.wikimedia.org',
                dt: new Date().toISOString(),
                id: uuidv1(),
                request_id: common.randomString(10),
                schema_uri: 'mediawiki/job/1',
                topic: 'mediawiki.job.htmlCacheUpdate',
                uri: 'https://commons.wikimedia.org/wiki/File:%D0%A1%D1%82%D0%B0%D0%B2%D0%BE%D0%BA_-_panoramio_(6).jpg'
            },
            params: {
                causeAction: 'page-edit',
                causeAgent: 'unknown',
                recursive: true,
                requestId: 'Wi7xIQpAANEAAEs6jxcAAACE',
                rootJobIsSelf: true,
                rootJobSignature: rootSignature,
                rootJobTimestamp: '20171211205706',
                table: 'redirect'
            },
            root_event: {
                dt: new Date().toISOString(),
                signature: rootSignature
            },
            sha1: common.randomString(10),
            type: 'htmlCacheUpdate'
        };
    },
    get refreshLinks() {
        const rootSignature = common.randomString(10);
        return {
            __proto__: eventMethods,
            database: 'zhwiki',
            mediawiki_signature: 'b2aad36ac3f784de69ad2809da3e82f6f1a08ab65f8542d626f556975fa6058c',
            meta: {
                domain: 'zh.wikipedia.org',
                dt: new Date().toISOString(),
                id: uuidv1(),
                request_id: common.randomString(10),
                schema_uri: 'mediawiki/job/1',
                topic: 'mediawiki.job.refreshLinks',
                uri: 'https://zh.wikipedia.org/wiki/Category:%E6%99%BA%E5%88%A9%E5%8D%9A%E7%89%A9%E9%A6%86'
            },
            params: {
                causeAction: 'update',
                causeAgent: 'uid:171544',
                requestId: '9c56c6dc077393ea1366c31a',
                rootJobSignature: rootSignature,
                rootJobTimestamp: '20180320163653'
            },
            root_event: {
                dt: new Date().toISOString(),
                signature: rootSignature
            },
            sha1: '9dfd2116c8c597c6b377a9100c670e21de20bf70',
            type: 'refreshLinks'
        };
    },
    get cdnPurge() {
        const releaseTimestamp = Date.now() / 1000 + 3;
        return {
            __proto__: eventMethods,
            database: 'commonswiki',
            delay_until: `${releaseTimestamp}`,
            mediawiki_signature: 'e6ff5af8f89ac6441c6ad7b34bdcf44fb1746c1ef6e07d8b9653c75d0005193e',
            meta: {
                domain: 'commons.wikimedia.org',
                dt: new Date().toISOString(),
                id: uuidv1(),
                request_id: common.randomString(10),
                schema_uri: 'mediawiki/job/1',
                topic: 'mediawiki.job.cdnPurge',
                uri: 'https://commons.wikimedia.org/wiki/Special:Badtitle/CdnCacheUpdate'
            },
            params: {
                jobReleaseTimestamp: releaseTimestamp,
                requestId: common.randomString(10),
                urls: [
                    'https://commons.wikimedia.org/wiki/Main_Page'
                ]
            },
            type: 'cdnPurge'
        };
    }
};

module.exports = common;
