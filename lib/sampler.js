'use strict';

/* eslint no-bitwise: ["error", { "allow": ["~", "<<"] }] */
/* jshint bitwise: false */

const murmur = require('murmur-32');
const Template = require('hyperswitch').Template;


class Sampler {
    constructor(options) {
        this._options = options || {};

        if (!this._options.hash_template) {
            throw new Error('Sampling requires that a hash_template be configured');
        }
        if (!this._options.rate) {
            throw new Error('Sampling requires that a rate be configured');
        }
        if ((this._options.rate > 1.0) || (this._options.rate < 0.01)) {
            throw new Error('Sample rate must be a value between 0.01 and 1.0');
        }

        this._hashSourceTemplate = new Template(this._options.hash_template);

        const percent = Math.round(this._options.rate * 100);
        const minMurmur = 1 << 31;
        const maxMurmur = ~minMurmur;
        const step = Math.round((Math.abs(minMurmur) + maxMurmur) / 100);
        this._maxHash = minMurmur + (step * percent);
    }

    /**
     * Returns true if this request should be sampled, false if it should
     * be ignored.
     * @param  {Object} context
     * @return {boolean}
     */
    accept(context) {
        const hashSource = this._hashSourceTemplate.expand(context);
        return (Sampler.hash(hashSource) <= this._maxHash);
    }

    /**
     * Returns a numeric representation of the value's murmur32 hash.
     * @param  {string} value
     * @return {number}
     */
    static hash(value) {
        return new DataView(murmur(value)).getInt32(0);
    }
}

module.exports = Sampler;
