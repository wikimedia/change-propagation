"use strict";

var P = require('bluebird');
var Rule = require('./rule');
var kafka = require('wmf-kafka-node');
var uuid = require('cassandra-uuid');
var RuleExecutor = require('./rule_executor');

/**
 * @const
 * @type {string}
 */
var ruleBroadcastTopic = 'change_propagation.rules_broadcast7';

function RuleManager(kafkaFactory, options) {
    this.log = options.log;
    this.kafkaFactory = kafkaFactory;
    this.rules = { };
}

RuleManager.prototype._onRuleReceived = function(hyper, ruleMessage) {
    var self = this;
    var ruleName = ruleMessage.key.toString();
    var ruleSpec = JSON.parse(ruleMessage.value);
    var oldSubscription = self.rules[ruleName];
    if (RuleExecutor.willChange(oldSubscription, ruleSpec)) {
        if (oldSubscription) {
            oldSubscription.unsubscribe();
        }
        self.rules[ruleName] = new RuleExecutor(new Rule(ruleName, ruleSpec), {
            kafkaFactory: self.kafkaFactory,
            hyper: hyper,
            log: self.log
        });
        return self.rules[ruleName].subscribe();
    }
};

RuleManager.prototype.init = function(hyper) {
    var self = this;
    return self.kafkaFactory.newProducer(self.kafkaFactory.newClient(), {
        partitionerType: 3 // Use keyed partitioner to support keying of rules
    })
    .then(function(producer) {
        self.producer = producer;
        return self.kafkaFactory.createTopics(producer, [ ruleBroadcastTopic ])
    })
    .then(function() {
        self.consumerClient = self.kafkaFactory.newClient();
        return self.kafkaFactory.getOffsetForTimestamp(
            self.consumerClient,
            ruleBroadcastTopic,
            Date.now() - 3600000
        );
    })
    .then(function(offset) {
        return self.kafkaFactory.newConsumer(self.consumerClient, ruleBroadcastTopic,
        'change-prop-rule-consumer-' + process.pid, offset);
    })
    .then(function(consumer) {
        self.consumer = consumer;
        self.consumer.on('message', self._onRuleReceived.bind(self, hyper));
    });
};

RuleManager.prototype.notifyNewRule = function(ruleName, ruleSpec) {
    var self = this;
    return new P(function(resolve, reject) {
        self.producer.send([
            {
                topic: ruleBroadcastTopic,
                messages: new kafka.KeyedMessage(ruleName, JSON.stringify(ruleSpec))
            }
        ], function(error, data) {
            if (error) {
                reject(error);
            } else {
                resolve(data);
            }
        });
    });
};

RuleManager.prototype.close = function() {
    var self = this;
    return P.try(function() {
        if (self.consumer) {
            self.consumer.close();
            self.consumer = undefined;
        }
        if (self.producer) {
            self.producer.close();
            self.producer = undefined;
        }
    })
    .then(function() {
        return P.all(Object.keys(self.rules).map(function(ruleName) {
            return self.rules[ruleName].unsubscribe();
        }));
    });
};

module.exports = RuleManager;
