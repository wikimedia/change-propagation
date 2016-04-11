"use strict";


/**
 * restbase-mod-queue-kafka main entry point
 */


var P = require('bluebird');
var kafka = P.promisifyAll(require('wmf-kafka-node'));
var RuleManager = require('../lib/rule_manager');


function Kafka(options) {
    this.log = options.log || function() {};
    this.conf = {
        uri: options.uri || 'localhost:2181',
        clientId: options.client_id || 'change-propagation'
    };
    this.ruleManager = new RuleManager(this);
}


Kafka.prototype.setup = function(hyper) {
    var self = this;
    self.hyper = hyper;
    self.conn = {};
    return self.ruleManager.init(hyper)
    .thenReturn({ status: 201 });
};

Kafka.prototype.addRule = function(hyper, req) {
    // TODO: Validation!
    return this.ruleManager.notifyNewRule(req.params.name, req.body)
    .then(function(emitRes) {
        return {
            status: 201,
            headers: {
                'content-type': 'application/json'
            },
            body: emitRes
        }
    });
};

module.exports = function(options) {

    var kafkaMod = new Kafka(options);

    return {
        spec: {
            paths: {
                '/setup': {
                    put: {
                        summary: 'set up the kafka listener',
                        operationId: 'setup_kafka'
                    }
                },
                '/rule/{name}': {
                    post: {
                        summary: 'add new rule',
                        operationId: 'add_rule'
                    }
                }
            }
        },
        operations: {
            setup_kafka: kafkaMod.setup.bind(kafkaMod),
            add_rule: kafkaMod.addRule.bind(kafkaMod)
        },
        resources: [{
            uri: '/queue/setup'
        }]
    };

};

