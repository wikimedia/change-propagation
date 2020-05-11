'use strict';

const RuleExecutor = require('./rule_executor');
const RetryExecutor = require('./retry_executor');
const Rule = require('./rule');
const P = require('bluebird');
const stringify = require('fast-json-stable-stringify');

class BasicSubscription {
    constructor(options, kafkaFactory, hyper, ruleName, ruleSpec) {
        this._kafkaFactory = kafkaFactory;
        this._options = options;
        this._hyper = hyper;
        ruleSpec.sample =  ruleSpec.sample || options.sample;
        this._rule = new Rule(ruleName, ruleSpec);
        this._subscribed = false;
        this._executor = new RuleExecutor(this._rule, this._kafkaFactory,
            hyper, this._options);
        this._retryExecutor = new RetryExecutor(this._rule, this._kafkaFactory,
            hyper, this._options);
    }

    subscribe() {
        if (this._subscribed) {
            throw new Error('Already subscribed!');
        }

        this._hyper.logger.log('info/subscription', {
            message: 'Subscribing based on basic topics',
            rule: this._rule.name,
            topics: this._rule.topics
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
// TODO: rewrite this one
class RegexTopicSubscription {
    constructor(options, kafkaFactory, hyper, ruleName, ruleSpec, metadataWatch) {
        this._kafkaFactory = kafkaFactory;
        this._options = options;
        this._hyper = hyper;
        this._ruleName = ruleName;
        this._ruleSpec = ruleSpec;
        ruleSpec.sample =  ruleSpec.sample || options.sample;
        this._topicTester = (ruleSpec.topics || (ruleSpec.topic && [ ruleSpec.topic ]))
        .map((topic) => {
            if (/^\/.+\/$/.test(topic)) {
                // Ok, we've got a regex topic! Compile the regex.
                return new RegExp(topic.substring(1, topic.length - 1));
            }
            return topic;
        });
        this._metadataWatch = metadataWatch;
        this._metadataWatch.on('topics_changed', (topics) => {
            const newFilteredTopics = this._filterTopics(topics);
            if (stringify(newFilteredTopics) !== stringify(this._filteredTopics)) {
                const removedTopics = this._filteredTopics.filter(topic =>
                    !newFilteredTopics.includes(topic));
                const addedTopics = newFilteredTopics.filter(topic =>
                    !this._filteredTopics.includes(topic));
                this._hyper.logger.log('info/topics_changed', {
                    message: 'Subscribed topics list changed',
                    added_topics: addedTopics,
                    removed_topics: removedTopics,
                    rule: this._ruleName
                });

                this.unsubscribe()
                .delay(5000) // Give some time for all the in-process consumption loops to finish up
                .then(() => this._subscribeTopics(newFilteredTopics));
            }
        });
        // Ignore the emitted errors - in 10 seconds it will retry
        this._metadataWatch.on('error', e => this._hyper.logger.log('error/metadata_refresh', e));

        this._subscribed = false;
        this._executors = [];
        this._filteredTopics = undefined;
    }

    /**
     * Filters out which topic names are ok to subscribe to.
     * @param {Array} proposedTopicNames a set of available topic names
     *                to check which to subscribe to
     * @return {Array}
     * @private
     */
    _filterTopics(proposedTopicNames) {
        return proposedTopicNames.filter((topic) => {
            return this._topicTester.some((topicTester) => {
                if (topicTester instanceof RegExp) {
                    return topicTester.test(topic);
                }
                return topicTester === topic;
            });
        }).sort();
    }

    _subscribeTopics(topicNames) {
        this._filteredTopics = topicNames;
        const topicRule = Rule.newWithTopicNames(this._ruleName,
            this._ruleSpec, this._filteredTopics);

        this._hyper.logger.log('info/subscription', {
            message: 'Subscribing based on regex',
            rule: this._ruleName,
            topics: topicRule.topics
        });

        const executor = new RuleExecutor(topicRule, this._kafkaFactory,
            this._hyper, this._options);
        this._executors.push(executor);

        const retryExecutor = new RetryExecutor(topicRule, this._kafkaFactory,
            this._hyper, this._options);
        this._executors.push(retryExecutor);

        return P.join(executor.subscribe(), retryExecutor.subscribe())
        .tap(() => {
            this._subscribed = true;
        });
    }

    subscribe() {
        return this._metadataWatch.getTopics()
        .then(topics => this._subscribeTopics(this._filterTopics(topics)));
    }

    unsubscribe() {
        if (this._subscribed) {
            this._subscribed = false;
            return P.each(this._executors, (executor => executor.close()));
        }
        return P.resolve();
    }
}

class Subscriber {
    constructor(options, kafkaFactory) {
        this._kafkaFactory = kafkaFactory;
        this._options = options;

        this._subscriptions = [];
        this._metadataWatch = undefined;
    }

    _createSubscription(hyper, ruleName, ruleSpec) {
        if (Rule.isBasicRule(ruleSpec)) {
            return P.resolve(new BasicSubscription(this._options,
                this._kafkaFactory, hyper, ruleName, ruleSpec));
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
            hyper, ruleName, ruleSpec, this._metadataWatch));
    }

    /**
     * Subscribe a rule spec under a certain rule name
     * @param {HyperSwitch} hyper the request dispatcher
     * @param {string} ruleName the name of the rule
     * @param {Object} ruleSpec the rule specification
     * @return {Promise}
     */
    subscribe(hyper, ruleName, ruleSpec) {
        return this._createSubscription(hyper, ruleName, ruleSpec)
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
