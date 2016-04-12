"use strict";

var P = require('bluebird');
var kafka = require('wmf-kafka-node');
var uuid = require('cassandra-uuid');

/**
 * Executor for the rule, subscribes to the topic and reacts to events
 *
 * @param {Rule} rule the rule to execute
 * @param {Object} context
 * @param {KafkaFactory} context.kafkaFactory
 * @param {Object} context.hyper
 * @param {Function} context.log
 * @constructor
 */
function RuleExecutor(rule, context) {
    this.rule = rule;
    this.context = context;
    this.log = context.log;
}

RuleExecutor.prototype._execEvent = function(event) {
    var self = this;
    var expander = {
        message: event,
        match: null
    };

    if (!self.rule.test(event)) {
        // no match, drop the message
        self.log('debug/' + self.rule.name, {
            msg: 'Dropping event message', event: event
        });
        return;
    }

    self.log('trace/' + self.rule.name, {
        msg: 'Event message received', event: event
    });
    expander.match = self.rule.expand(event);

    return P.each(self.rule.exec, P.method(function(tpl) {
        console.log('EXECUTION!!!');
        return self.context.hyper.request(tpl.expand(expander));
    })).catch(function(err) {

        self.log('info/' + self.rule.name, err);
    });
};

RuleExecutor.prototype.subscribe = function() {
    var self = this;
    if (self.rule.noop) { return P.resolve(); }

    return self.context.kafkaFactory.newConsumer(self.context.kafkaFactory.newClient(),
        self.rule.topic, 'change-prop-' + self.rule.name)
    .then(function(consumer) {
        self.consumer = consumer;
        self.consumer.on('message', function(message) {
            console.log(message);
            return self._execEvent(JSON.parse(message.value));
        });
        self.consumer.on('topics_changed', function(topicList) {
            // only one topic can be subscribed to by this client
            if (topicList && topicList.length) {
                self.log('info/subscription', {
                    rule: self.rule.name,
                    msg: 'Listening to ' + topicList[0]
                });
            } else {
                self.log('info/subscription', {
                    rule: self.rule.name,
                    msg: 'Lost ownership of ' + self.rule.topic
                });
            }
        });
        self.consumer.on('error', function(err) {
            self.log('warn/error', {
                err: err,
                rule: self.rule.name
            });
        });
    });
};

RuleExecutor.prototype.unsubscribe = function() {
    var self = this;
    if (self.rule.noop) {
        return P.resolve();
    }
    // TODO
};

RuleExecutor.willChange = function(currentExecutor, proposedRuleSpec) {
    if (!currentExecutor) {
        return true;
    }
    return !currentExecutor.rule.isEqualSpec(proposedRuleSpec);
};

module.exports = RuleExecutor;