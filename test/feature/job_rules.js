"use strict";

const ChangeProp = require('../utils/changeProp');
const nock = require('nock');
const common = require('../utils/common');
const P = require('bluebird');

process.env.UV_THREADPOOL_SIZE = 128;

describe('JobQueue rules', function() {
    this.timeout(30000);

    const changeProp = new ChangeProp('config.jobqueue.wikimedia.yaml');
    let producer;

    before(function() {
        // Setting up might take some tome, so disable the timeout
        this.timeout(50000);
        return changeProp.start()
        .then(() => common.factory.createProducer({ log: console.log.bind(console) }))
        .then(result => producer = result);
    });

    [
        'updateBetaFeaturesUserCounts'
    ].forEach((jobType) => {
        it(`Should propagate ${jobType} job`, () => {
            const sampleEvent = common.jobs[jobType];
            const service = nock('http://jobrunner.wikipedia.org', {
                reqheaders: {
                    host: sampleEvent.meta.domain,
                    'content-type': 'application/json'
                }
            })
            .post('/wiki/Special:RunSingleJob', sampleEvent)
            .reply({});
            return producer.produce(`test_dc.mediawiki.job.${jobType}`, 0,
                Buffer.from(JSON.stringify(sampleEvent)))
            .then(() => common.checkAPIDone(service))
            .finally(() => nock.cleanAll());
        });
    });

    it('Should support partitioned refreshLinks', () => {
        const sampleEvent = common.jobs.refreshLinks;
        const sampleEventCopy = JSON.parse(JSON.stringify(sampleEvent));
        sampleEventCopy.meta.topic = 'change-prop.partitioned.mediawiki.job.refreshLinks';
        const service = nock('http://jobrunner.wikipedia.org', {
            reqheaders: {
                host: sampleEvent.meta.domain,
                'content-type': 'application/json'
            }
        })
        .post('/wiki/Special:RunSingleJob', sampleEventCopy)
        .reply({});
        return producer.produce(`test_dc.mediawiki.job.refreshLinks`, 0,
            Buffer.from(JSON.stringify(sampleEvent)))
        .then(() => common.checkAPIDone(service))
        .finally(() => nock.cleanAll());
    });

    it('Should deduplicate based on ID', () => {
        const sampleEvent = common.jobs.updateBetaFeaturesUserCounts;
        const service = nock('http://jobrunner.wikipedia.org', {
            reqheaders: {
                host: sampleEvent.meta.domain,
                'content-type': 'application/json'
            }
        })
        .post('/wiki/Special:RunSingleJob', sampleEvent)
        .twice() // We set 2 mocks here in order to check that after the test 1 is still pending
        .reply({});
        return P.each([
            JSON.stringify(sampleEvent),
            JSON.stringify(sampleEvent)
        ].map(strMsg => Buffer.from(strMsg)), msg =>
            producer.produce('test_dc.mediawiki.job.updateBetaFeaturesUserCounts', 0, msg))
        .then(() => common.checkPendingMocks(service, 1))
        .finally(() => nock.cleanAll());
    });

    it('Should deduplicate based on SHA1', () => {
        const firstEvent = common.jobs.updateBetaFeaturesUserCounts;
        const secondEvent = common.jobs.updateBetaFeaturesUserCounts;
        secondEvent.meta.dt = new Date(Date.now() - 1000).toISOString();
        secondEvent.sha1 = firstEvent.sha1;
        const service = nock('http://jobrunner.wikipedia.org', {
            reqheaders: {
                host: firstEvent.meta.domain,
                'content-type': 'application/json'
            }
        })
        .post('/wiki/Special:RunSingleJob', firstEvent)
        .reply({})
        // We specify a mock for the second event as well to checkout it's still pending after tests
        .post('/wiki/Special:RunSingleJob', secondEvent)
        .reply({});
        return producer.produce('test_dc.mediawiki.job.updateBetaFeaturesUserCounts', 0, Buffer.from(JSON.stringify(firstEvent)))
        .then(() => common.checkPendingMocks(service, 1))
        .then(() => producer.produce('test_dc.mediawiki.job.updateBetaFeaturesUserCounts', 0, Buffer.from(JSON.stringify(secondEvent))))
        .then(() => common.checkPendingMocks(service, 1))
        .finally(() => nock.cleanAll());
    });

    it('Should deduplicate based on SHA1 and root job combination', () => {
        const firstEvent = common.jobs.htmlCacheUpdate;
        const secondEvent = common.jobs.htmlCacheUpdate;
        secondEvent.sha1 = firstEvent.sha1;
        secondEvent.meta.dt = new Date(Date.now() + 10000).toISOString();
        secondEvent.root_event.dt = new Date(Date.now() - 10000).toISOString();
        const service = nock('http://jobrunner.wikipedia.org', {
            reqheaders: {
                host: firstEvent.meta.domain,
                'content-type': 'application/json'
            }
        })
        .post('/wiki/Special:RunSingleJob', firstEvent)
        .reply({})
        // We specify a mock for the second event as well to checkout it's still pending after tests
        .post('/wiki/Special:RunSingleJob', secondEvent)
        .reply({});
        return producer.produce('test_dc.mediawiki.job.htmlCacheUpdate', 0, Buffer.from(JSON.stringify(firstEvent)))
        .then(() => common.checkPendingMocks(service, 1))
        .then(() => producer.produce('test_dc.mediawiki.job.htmlCacheUpdate', 0, Buffer.from(JSON.stringify(secondEvent))))
        .then(() => common.checkPendingMocks(service, 1))
        .finally(() => nock.cleanAll());
    });

    it('Should deduplicate base on root job', () => {
        const firstEvent = common.jobs.htmlCacheUpdate;
        const secondEvent = common.jobs.htmlCacheUpdate;
        secondEvent.root_event.signature = firstEvent.root_event.signature;
        secondEvent.root_event.dt = new Date(new Date(firstEvent.root_event.dt) - 1000).toISOString();
        const service = nock('http://jobrunner.wikipedia.org', {
            reqheaders: {
                host: firstEvent.meta.domain,
                'content-type': 'application/json'
            }
        })
        .post('/wiki/Special:RunSingleJob', firstEvent)
        .reply({})
        // We specify a mock for the second event as well to checkout it's still pending after tests
        .post('/wiki/Special:RunSingleJob', secondEvent)
        .reply({});
        return producer.produce('test_dc.mediawiki.job.htmlCacheUpdate', 0, Buffer.from(JSON.stringify(firstEvent)))
        .then(() => common.checkPendingMocks(service, 1))
        .then(() => producer.produce('test_dc.mediawiki.job.htmlCacheUpdate', 0, Buffer.from(JSON.stringify(secondEvent))))
        .then(() => common.checkPendingMocks(service, 1))
        .finally(() => nock.cleanAll());
    });

    after(() => changeProp.stop());
});

