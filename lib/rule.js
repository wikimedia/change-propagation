'use strict';

const stringify = require('json-stable-stringify');
const HyperSwitch = require('hyperswitch');
const Template = HyperSwitch.Template;

const DEFAULT_RETRY_DELAY = 500;
const DEFAULT_RETRY_FACTOR = 6;
const DEFAULT_RETRY_LIMIT = 5;

/**
 * Creates a JS function that verifies property equality
 *
 * @param retryDefinition the condition in the format of 'retry_on' stanza
 * @returns {Function} a function that verifies the condition
 */
function _compileRetryCondition(retryDefinition) {
    function createCondition(retryCond, option) {
        if (retryCond === 'status') {
            const opt = option.toString();
            if (/^[0-9]+$/.test(opt)) {
                return `(res["${retryCond}"] === ${opt})`;
            }
            if (/^[0-9x]+$/.test(opt)) {
                return `/^${opt.replace(/x/g, "\\d")}$/.test(res["${retryCond}"])`;
            }
            throw new Error(`Invalid retry_on condition ${opt}`);
        } else {
            return `(stringify(res["${retryCond}"]) === '${stringify(option)}')`;
        }
    }

    const condition = [];
    Object.keys(retryDefinition).forEach((catchCond) => {
        if (Array.isArray(retryDefinition[catchCond])) {
            const orCondition = retryDefinition[catchCond].map((option) =>
                createCondition(catchCond, option));
            condition.push('(' + orCondition.join(' || ') + ')');
        } else {
            condition.push(createCondition(catchCond, retryDefinition[catchCond]));
        }
    });
    const code = `return (${condition.join(' && ')});`;
    /* jslint evil: true */
    return new Function('stringify', 'res', code).bind(null, stringify);
}

function _getMatchObjCode(obj) {
    if (obj.constructor === Object) {
        return '{' + Object.keys(obj).map((key) => {
            const field = obj[key];
            return key + ': ' + _getMatchObjCode(obj[key]);
        }).join(', ') + '}';
    }
    return obj;
}

function _compileArrayMatch(obj, result, name) {
    const itemsCheck = obj.map((item, index) =>
        `${name}.find((item) => ${_compileMatch(item, {}, 'item', index)})`).join(' && ');
    return `Array.isArray(${name})` + (itemsCheck.length ? (' && ' + itemsCheck) : '');
}

function _compileMatch(obj, result, name, fieldName) {
    if (obj.constructor !== Object) {
        if (Array.isArray(obj)) {
            return _compileArrayMatch(obj, result, name, fieldName);
        }
        if (obj === 'undefined') {
            return `${name} === undefined`;
        }
        if (typeof obj !== 'string') {
            // not a string, so it has to match exactly
            result[fieldName] = obj;
            return `${name} === ${obj}`;
        }
        if (obj[0] !== '/' && obj[obj.length - 1] !== '/') {
            // not a regex, quote the string
            result[fieldName] = `'${obj}'`;
            return `${name} === '${obj}'`;
        }
        // it's a regex, we have to the test the arg
        result[fieldName] = `${obj}.exec(${name})`;
        return `${obj}.test(${name})`;
    }

    // this is an object, we need to split it into components
    const subObj = fieldName ? {} : result;
    const test = Object.keys(obj).map(
        (key) => _compileMatch(obj[key], subObj, `${name}['${key}']`, key))
    .join(' && ');
    if (fieldName) {
        result[fieldName] = subObj;
    }
    return test;
}

class Rule {
    constructor(name, spec) {
        this.name = name;
        this.spec = spec || {};

        this.topic = this.spec.topic;
        if (!this.topic) {
            throw new Error(`No topic specified for rule ${this.name}`);
        }
        this.spec.retry_on = this.spec.retry_on || {
            status: [ '50x' ]
        };
        this.shouldRetry = _compileRetryCondition(this.spec.retry_on);
        this.exec = this._processExec(this.spec.exec);
        this._match = this._processMatch(this.spec.match);
        this._match_not = (this._processMatch(this.spec.match_not) || {}).test;
        this.spec.retry_delay = this.spec.retry_delay || DEFAULT_RETRY_DELAY;
        this.spec.retry_limit = this.spec.retry_limit || DEFAULT_RETRY_LIMIT;
        this.spec.retry_factor = this.spec.retry_factor || DEFAULT_RETRY_FACTOR;
    }

    /**
     * Tests the message against the compiled evaluation test
     *
     * @param {Object} message the message to test
     * @return true if no match is set for this rule or if the message matches
     */
    test(message) {
        var match = true;
        if (this._match && this._match.test) {
            match = this._match.test(message);
        }
        if (this._match_not) {
            match = match && !this._match_not(message);
        }
        return match;
    }

    /**
     * Expands the rule's match object with the given message's content
     *
     * @param {Object} message the message to use in the expansion
     * @return {Object} the object containing the expanded match portion of the rule
     */
    expand(message) {
        return this._match && this._match.expand ? this._match.expand(message) : {};
    }

    _processMatch(match) {
        if (!match) {
            // No particular match specified, so we
            // should accept all events for this topic
            return;
        }

        const obj = {};
        const test = _compileMatch(match, obj, 'message');
        try {
            return {
                /* jslint evil: true  */
                test: new Function('message', 'return ' + test),
                /* jslint evil: true  */
                expand: new Function('message', 'return ' + _getMatchObjCode(obj))
            };
        } catch (e) {
            throw new Error('Invalid match object given!');
        }
    }

    _processExec(exec) {
        if (!exec) {
            // nothing to do, the rule is a no-op
            this.noop = true;
            return;
        }

        if (!Array.isArray(exec)) {
            exec = [exec];
        }

        const templates = [];
        for (let idx = 0; idx < exec.length; idx++) {
            const req = exec[idx];
            if (req.constructor !== Object || !req.uri) {
                throw new Error(`In rule ${this.name}, request number ${idx}
                    must be an object and must have the "uri" property`);
            }
            req.method = req.method || 'get';
            req.headers = req.headers || {};
            templates.push(new Template(req));
        }
        return templates;
    }
}

module.exports = Rule;
