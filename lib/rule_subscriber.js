"use strict";

const RuleExecutor = require('./rule_executor');
const RetryExecutor = require('./retry_executor');
const P = require('bluebird');

class BasicSubscription {
    constructor(options, kafkaFactory, hyper, rule) {
        this._kafkaFactory = kafkaFactory;
        this._options = options;
        this._log = this._options.log || (() => {});
        this._rule = rule;

        this._subscribed = false;
        this._executor = new RuleExecutor(rule, this._kafkaFactory,
            hyper, this._log, this._options);
        this._retryExecutor = new RetryExecutor(rule, this._kafkaFactory,
            hyper, this._log, this._options);
    }

    subscribe() {
        if (this._subscribed) {
            throw new Error('Already subscribed!');
        }

        this._log('info/subscription', {
            message: 'Subscribing based on basic topic',
            rule: this._rule.name,
            topic: this._rule.topic
        });

        return P.join(this._executor.subscribe(), this._retryExecutor.subscribe())
        .tap(() => {
            this._subscribed = true;
        });
    }

    unsubscribe() {
        if (this._subscribed) {
            this._executor.close();
            this._retryExecutor.close();
        }
    }
}

class RegexTopicSubscription {
    constructor(options, kafkaFactory, hyper, rule, metadataWatch) {
        this._kafkaFactory = kafkaFactory;
        this._options = options;
        this._log = this._options.log || (() => {});
        this._hyper = hyper;
        this._rule = rule;
        this._metadataWatch = metadataWatch;
        this._metadataWatch.on('topic_added', (topic) => {
            if (this._rule.topic.test(topic)) {
                this._subscribeTopic(topic);
            }
        });

        this._subscribed = false;
        this._executors = [];
    }

    _subscribeTopic(topicName) {
        const topicRule = this._rule.clone(topicName);

        this._log('info/subscription', {
            message: 'Subscribing based on regex',
            rule: this._rule.name,
            topic: topicName
        });

        const executor = new RuleExecutor(topicRule, this._kafkaFactory,
            this._hyper, this._log, this._options);
        this._executors.push(executor);

        const retryExecutor = new RetryExecutor(topicRule, this._kafkaFactory,
            this._hyper, this._log, this._options);
        this._executors.push(retryExecutor);

        return P.join(executor.subscribe(), retryExecutor.subscribe())
        .tap(() => {
            this._subscribed = true;
        });
    }

    subscribe() {
        return this._metadataWatch.getTopics()
        .then((topics) => {
            const selectedTopics = topics.filter(topic => this._rule.topic.test(topic));

            return P.each(selectedTopics, (topicName) => {
                return this._subscribeTopic(topicName);
            });
        });
    }

    unsubscribe() {
        if (this._subscribed) {
            this._executors.forEach(executor => executor.close());
        }
    }
}

class Subscriber {
    constructor(options, kafkaFactory) {
        this._kafkaFactory = kafkaFactory;
        this._options = options;

        this._subscriptions = [];
        this._metadataWatch = undefined;
    }

    _createSubscription(hyper, rule) {
        if (!rule.isRegexRule()) {
            return P.resolve(new BasicSubscription(this._options,
                this._kafkaFactory, hyper, rule));
        }
        let maybeCreateWatchAction;
        if (!this._metadataWatch) {
            maybeCreateWatchAction =
                this._kafkaFactory.createMetadataWatch('metadata_refresher')
                .tap((refresher) => {
                    this._metadataWatch = refresher;
                });
        } else {
            maybeCreateWatchAction = P.resolve(this._metadataWatch);
        }

        return maybeCreateWatchAction
        .then(() => new RegexTopicSubscription(this._options, this._kafkaFactory,
            hyper, rule, this._metadataWatch));
    }

    subscribe(hyper, rule) {
        return this._createSubscription(hyper, rule)
        .then((subscription) => {
            this._subscriptions.push(subscription);
            return subscription.subscribe();
        });
    }

    unsubscribeAll() {
        this._subscriptions.forEach(subscription => subscription.unsubscribe());
        if (this._metadataWatch) {
            this._metadataWatch.disconnect();
        }
    }
}

module.exports = Subscriber;
