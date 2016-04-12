'use strict';


var HyperSwitch = require('hyperswitch');
var stringify = require('json-stable-stringify');
var Template = HyperSwitch.Template;


function Rule(name, spec) {
    this.name = name;
    this.spec = spec || {};
    this.exec = [];
    this.topic = '';
    this.match = {
        all: true
    };
    this.noop = false;

    this._processRule();

}


/**
 * Tests the message against the compiled evaluation test
 *
 * @param {Object} message the message to test
 * @return true if no match is set for this rule or if the message matches
 */
Rule.prototype.test = function(message) {

    return this.match.all || this._match_test(message);

};


/**
 * Expands the rule's match object with the given message's content
 *
 * @param {Object} message the message to use in the expansion
 * @return {Object} the object containing the expanded match portion of the rule
 */
Rule.prototype.expand = function(message) {

    return this._match_expand(message);

};


Rule.prototype._match_test = function(message) {

    // NOTE: this method gets overriden by
    // each Rule instance with a defined match object
    return false;

};


Rule.prototype._match_expand = function(message) {

    // NOTE: this method gets overriden by
    // each Rule instance with a defined match object
    return {};

};


Rule.prototype._compileMatch = function(obj, result, name, fieldName) {

    var self = this;

    if (obj.constructor !== Object) {
        if (typeof obj !== 'string') {
            // not a string, so it has to match exactly
            result[fieldName] = obj;
            return name + ' === ' + obj;
        }
        if (obj[0] !== '/' && obj[obj.length - 1] !== '/') {
            // not a regex, quote the string
            result[fieldName] = "'" + obj + "'";
            return name + " === '" + obj + "'";
        }
        // it's a regex, we have to the test the arg
        result[fieldName] = obj + '.exec(' + name + ')';
        return obj + '.test(' + name + ')';
    }

    // this is an object, we need to split it into components
    var test = '';
    var subObj = fieldName ? {} : result;
    Object.keys(obj).forEach(function(key) {
        var subTest = self._compileMatch(obj[key], subObj, name + "['" + key + "']", key);
        if (test.length) {
            test += ' && ';
        }
        test += subTest;
    });
    if (fieldName) {
        result[fieldName] = subObj;
    }

    return test;

};


Rule.prototype._getMatchObjCode = function(obj) {

    var self = this;
    var code = '{';

    if (obj.constructor !== Object) {
        return '';
    }

    Object.keys(obj).forEach(function(key) {
        var field = obj[key];
        var fieldCode = key + ': ';
        if (field.constructor === Object) {
            fieldCode += self._getMatchObjCode(field);
        } else {
            fieldCode += field;
        }
        if (code.length > 1) {
            code += ',';
        }
        code += fieldCode;
    });

    return code + '}';

};


Rule.prototype._processMatch = function() {

    var test = '';
    var obj = {};

    // no particual match specified, so we
    // should accept all events for this topic
    if (!this.spec.match) {
        return;
    }

    this.match.all = false;
    test = this._compileMatch(this.spec.match, obj, 'message');
    try {
        /* jslint evil: true  */
        this._match_test = new Function('message', 'return ' + test);
        /* jslint evil: true  */
        this._match_expand = new Function('message', 'return ' +
            this._getMatchObjCode(obj));
    } catch (e) {
        throw new Error('Invalid match object given!');
    }

};


Rule.prototype._processRule = function() {

    var idx;
    var reqs;
    var templates = [];

    // validate the rule spec
    if (!this.spec.topic) {
        throw new Error('No topic specified for rule ' + this.name);
    }
    if (!this.spec.exec) {
        // nothing to do, the rule is a no-op
        this.noop = true;
        return;
    }
    if (!Array.isArray(this.spec.exec)) {
        this.spec.exec = [this.spec.exec];
    }

    reqs = this.spec.exec;
    for (idx = 0; idx < reqs.length; idx++) {
        var req = reqs[idx];
        if (req.constructor !== Object || !req.uri) {
            throw new Error('In rule ' + this.name + ', request number ' + idx +
                ' must be an object and must have the "uri" property');
        }
        req.method = req.method || 'get';
        req.headers = req.headers || {};
        templates.push(new Template(req));
    }

    this._processMatch();

    this.topic = this.spec.topic;
    this.exec = templates;

};

Rule.prototype.isEqualSpec = function(thatSpec) {
    if (!thatSpec) {
        return false;
    }

    return stringify(this.spec) === stringify(thatSpec);
};

module.exports = Rule;

