"use strict";

const ChangeProp = require('../utils/changeProp');
const nock = require('nock');
const common = require('../utils/common');

process.env.UV_THREADPOOL_SIZE = 128;

describe('JobQueue rules', function () {
    this.timeout(30000);

    const changeProp = new ChangeProp('config.jobqueue.wikimedia.yaml');
    let producer;

    before(function() {
        // Setting up might take some tome, so disable the timeout
        this.timeout(50000);
        return changeProp.start()
        .then(() => common.factory.createProducer(console.log.bind(console)))
        .then((result) => producer = result);
    });

    [
        'updateBetaFeaturesUserCounts'
    ].forEach((jobType) => {
        it(`Should propagate ${jobType} job`, () => {
            const service = nock('http://jobrunner.wikipedia.org', {
                reqheaders: {
                    host: common.jobs[jobType].meta.domain,
                    'content-type': 'application/json'
                }
            })
            .post('/wiki/Special:RunSingleJob', common.jobs[jobType]).reply({});
            return producer.produce(`test_dc.mediawiki.job.${jobType}`, 0,
                Buffer.from(JSON.stringify(common.jobs[jobType])))
            .then(() => common.checkAPIDone(service))
            .finally(() => nock.cleanAll());
        });
    });

    after(() => changeProp.stop());
});

