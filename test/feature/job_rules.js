'use strict';

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
        'updateBetaFeaturesUserCounts',
        'cdnPurge'
    ].forEach((jobType) => {
        it(`Should propagate ${jobType} job`, function() {
            this.timeout(10000);
            const sampleEvent = common.jobs[jobType];
            const service = nock('http://jobrunner.wikipedia.org', {
                reqheaders: {
                    host: sampleEvent.meta.domain,
                    'content-type': 'application/json'
                }
            })
            .post('/wiki/Special:RunSingleJob', sampleEvent)
            .reply({});
            return producer.produce(`test_dc.mediawiki.job.${jobType}`, 0, sampleEvent.toBuffer())
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
        return producer.produce(`test_dc.mediawiki.job.refreshLinks`, 0, sampleEvent.toBuffer())
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
        return P.each([ sampleEvent,  sampleEvent ], msg =>
            producer.produce('test_dc.mediawiki.job.updateBetaFeaturesUserCounts', 0, msg.toBuffer()))
        .then(() => common.checkPendingMocks(service, 1))
        .finally(() => nock.cleanAll());
    });

    it('Should deduplicate based on SHA1', () => {
        const firstEvent = common.jobs.updateBetaFeaturesUserCounts;
        const secondEvent = common.jobs.updateBetaFeaturesUserCounts;
        secondEvent.meta.dt = new Date(Date.now() - 5000).toISOString();
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
        return producer.produce('test_dc.mediawiki.job.updateBetaFeaturesUserCounts', 0, firstEvent.toBuffer())
        .then(() => common.checkPendingMocks(service, 1))
        .then(() => producer.produce('test_dc.mediawiki.job.updateBetaFeaturesUserCounts', 0, secondEvent.toBuffer()))
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
        return producer.produce('test_dc.mediawiki.job.htmlCacheUpdate', 0, firstEvent.toBuffer())
        .then(() => common.checkPendingMocks(service, 1))
        .then(() => producer.produce('test_dc.mediawiki.job.htmlCacheUpdate', 0, secondEvent.toBuffer()))
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
        return producer.produce('test_dc.mediawiki.job.htmlCacheUpdate', 0, firstEvent.toBuffer())
        .then(() => common.checkPendingMocks(service, 1))
        .then(() => producer.produce('test_dc.mediawiki.job.htmlCacheUpdate', 0, secondEvent.toBuffer()))
        .then(() => common.checkPendingMocks(service, 1))
        .finally(() => nock.cleanAll());
    });

    it('Should support delayed jobs with re-enqueue', () => {
        this.timeout(20000);
        const sampleEvent = common.jobs.cdnPurge;
        sampleEvent.delay_until = `${parseInt(sampleEvent.delay_until) + 10}`;
        const service = nock('http://jobrunner.wikipedia.org', {
            reqheaders: {
                host: sampleEvent.meta.domain,
                'content-type': 'application/json'
            }
        })
        .post('/wiki/Special:RunSingleJob', sampleEvent)
        .reply({});
        return producer.produce(`test_dc.mediawiki.job.cdnPurge`, 0, sampleEvent.toBuffer())
        .then(() => common.checkAPIDone(service))
        .finally(() => nock.cleanAll());
    });

    after(() => changeProp.stop());
});

