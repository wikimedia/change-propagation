'use strict';

const assert  = require('assert');
const common  = require('../utils/common');
const Sampler = require('../../lib/sampler');
const CHARS   = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';


function makeRandomString(length) {
    let text = '';
    for (let i = 0; i < length; i++) {
	text += CHARS.charAt(Math.floor(Math.random() * CHARS.length));
    }
    return text;
}

describe('Sampler', () => {
    it('Should accept the correct number of values', () => {
        const rate = 0.20;    // Accept 20%
        const corpusLen = 10000;
        const template = '{{message.meta.domain}}-{{message.page_title}}';
        const sampler = new Sampler({ rate: rate, hash_template: template});

        let count = 0;
        for (let i = 0; i < corpusLen; i++) {
            const context = {
                message: common.eventWithProperties('on_topic', {
                    page_title: makeRandomString(12),
                    message: 'sampled'
                }),
                match: {
                    meta: {
                        uri: 'https://en.wikipedia.org/wiki/SamplePage'
                    },
                    message: 'sampled'
                }
            };
            if (sampler.accept(context)) {
                count += 1;
            }
        }

        const accepted = (count / corpusLen) * 100;
        const deviation = Math.abs(accepted - (rate * 100));
        assert.ok(deviation < 1, 'Sampling should be accurate to with 1%');
    });
});
