'use strict';

const P = require('bluebird');
const EventEmitter = require('events').EventEmitter;
const fs = require('fs');

const topics = fs.readFileSync(`${__dirname}/test_topics`, 'utf8')
    .split('\n')
    .map((line) => line.split(' ')[0].replace(/^test_dc\./, ''))
    .filter((line) => line.length);

class MockMetadataWatch extends EventEmitter {
    getTopics() {
        return P.resolve(topics);
    }
    disconnect() {}
}

class MockProducer {
    constructor(messages) {
        this._messages = messages;
    }
    produce(topic, partition, message) {
        if (!this._messages.has(topic)) {
            this._messages.set(topic, []);
        }
        this._messages.get(topic).push({
            topic,
            partition,
            value: message
        });
        return P.resolve();
    }
    disconnect() {}
}

class MockConsumer extends EventEmitter {
    constructor(messages, topics = []) {
        super();
        this._topics = topics;
        this._messages = messages;
        this._currentTopicOffsets = new Map();
    }
    _getCurrentOffset(topic) {
        if (this._currentTopicOffsets.has(topic)) {
            return this._currentTopicOffsets.get(topic);
        }
        return 0;
    }
    disconnect() {}
    disconnectAsync() {}

    consumeAsync() {
        for (const topic of this._topics) {
            if (this._messages.has(topic)) {
                const topicMessages = this._messages.get(topic);
                const currentTopicOffset = this._getCurrentOffset(topic);
                if (topicMessages.length > currentTopicOffset) {
                    this._currentTopicOffsets.set(topic, currentTopicOffset + 1);
                    return P.resolve([
                        Object.assign(
                            topicMessages[currentTopicOffset],
                            { offset: currentTopicOffset }
                        )
                    ]);
                }
            }
        }
        return P.resolve([]);
    }
    commitMessageAsync() {
        return P.resolve();
    }
}

class MockKafkaFactory {
    constructor() {
        this._messages = new Map();
    }

    /**
     * Returns a DC name to consume from
     *
     * @return {string}
     */
    get consumeDC() {
        // TODO
        return 'test_dc';
    }

    /**
     * Returns a DC name to produce to
     *
     * @return {string}
     */
    get produceDC() {
        // TODO
        return 'test_dc';
    }

    /**
     * Create new KafkaConsumer and connect it.
     *
     * @param {string} groupId Consumer group ID to use
     * @param {Array} topics Topics to subscribe to
     * @return {Object} kafka consumer
     */
    createConsumer(groupId, topics) {
        return P.resolve(new MockConsumer(this._messages, topics));
    }

    createMetadataWatch() {
       return P.resolve(new MockMetadataWatch());
    }

    createProducer() {
        return P.resolve(new MockProducer(this._messages));
    }

    createGuaranteedProducer() {
        return this.createProducer();
    }
}

module.exports = MockKafkaFactory;
