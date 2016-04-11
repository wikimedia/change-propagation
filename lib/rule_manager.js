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
var ruleBroadcastTopic = 'change_propagation.rules_broadcast6';

function RuleManager(options) {
    this.log = options.log;
    this.conf = options.conf;
    this.rules = { };
}

RuleManager.prototype._onRuleReceived = function(hyper, ruleMessage) {
    var self = this;
    var ruleName = ruleMessage.key.toString();
    var ruleSpec = JSON.parse(ruleMessage.value);
    var oldRule = self.rules[ruleName];
    if (!oldRule || !oldRule.rule.isEqual(ruleSpec)) {
        // TODO: unSubscribe old rule
        var newRule = new Rule(ruleName, ruleSpec);
        self.rules[ruleName] = new RuleExecutor(newRule, self.conf, hyper, self.log);
        console.log('Rule update consumed by pid=' + process.pid, newRule);
        return self.rules[ruleName].subscribe();
    }
};

RuleManager.prototype._setUpRuleConsumer = function(hyper, offset) {
    var self = this;
    return new P(function(resolve, reject) {
        self.consumer = new kafka.HighLevelConsumer(
        self.client,
        [{
            topic: ruleBroadcastTopic,
            offset: offset
        }],
        { groupId: 'change-prop-rule-consumer-' + process.pid }
        );
        self.consumer.on('error', reject);
        self.consumer.on('rebalanced', resolve);
        self.consumer.on('message', self._onRuleReceived.bind(self, hyper));
    });
};

RuleManager.prototype._setUpRuleProducer = function() {
    var self = this;
    self.prodClient = new kafka.Client(self.conf.uri,
        self.conf.clientId + '-' + uuid.TimeUuid.now() + '-' + uuid.Uuid.random(),
        {}
    );
    return new P(function(resolve, reject) {
        self.producer = new kafka.HighLevelProducer(self.prodClient, {
            // Use keyed partitioner
            partitionerType: 3
        });
        self.producer.on('ready', resolve);
        self.producer.on('error', reject);
    })
};

RuleManager.prototype._createRuleTopic = function() {
    var self = this;
    return new P(function(resolve, reject) {
        self.producer.createTopics([ ruleBroadcastTopic ], false, function (err, data) {
            if (err) {
                reject(err);
            } else {
                resolve(data);
            }
        });
    });
};

RuleManager.prototype.init = function(hyper) {
    var self = this;
    return self._setUpRuleProducer()
    .then(self._createRuleTopic.bind(self))
    .then(function() {
        return new P(function(resolve, reject) {
            self.client = new kafka.Client(self.conf.uri,
            self.conf.clientId + '-' + uuid.TimeUuid.now() + '-' + uuid.Uuid.random(),
            {}
            );
            self.offset = new kafka.Offset(self.client);
            var oneHourAgo = Date.now() - 3600000;
            self.offset.on('connect', function() {
                self.offset.fetch([{
                    topic: ruleBroadcastTopic,
                    time: oneHourAgo,
                    maxNum: 1
                }], function(err, data) {
                    if (err) {
                        reject(err);
                    } else {
                        var offsetList = data[ruleBroadcastTopic]['0'];
                        resolve(offsetList.length ? offsetList[0] : 0);
                    }
                });
            });
        });
    })
    .then(self._setUpRuleConsumer.bind(self, hyper));
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
