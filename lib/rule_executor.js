"use strict";

var P = require('bluebird');
var kafka = require('wmf-kafka-node');
var uuid = require('cassandra-uuid');

function RuleExecutor(rule, kafkaConf, hyper, log) {
    this.rule = rule;
    this.kafkaConf = kafkaConf;
    this.hyper = hyper;
    this.log = log;
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
        console.log(tpl.expand(expander));
        return self.hyper.request(tpl.expand(expander));
    })).catch(function(err) {

        self.log('info/' + self.rule.name, err);
    });
};

RuleExecutor.prototype.subscribe = function() {
    var self = this;
    if (self.rule.noop) {
        return P.resolve();
    }

    self.client = new kafka.Client(
        self.kafkaConf.uri,
        self.kafkaConf.clientId + '-' + uuid.TimeUuid.now() + '-' + uuid.Uuid.random(),
        {}
    );
    self.consumer = new kafka.HighLevelConsumer(
        self.client,
        [{ topic: self.rule.topic }],
        { groupId: 'change-prop-' + self.rule.name }
    );
    self.consumer.on('message', function(message) {
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
    return new P(function(resolve) {
        self.consumer.on('rebalanced', resolve);
    });
};

RuleExecutor.prototype.unsubscribe = function() {
    var self = this;
    if (self.rule.noop) {
        return P.resolve();
    }
};

module.exports = RuleExecutor;