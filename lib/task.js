"use strict";

/**
 * Represents a task for TaskQueue
 */
class Task {
    /**
     * Constructs a new instance of the task
     *
     * @param {HighLevelConsumer} consumer the consumer which the message was emitted by
     * @param {Object} message the event message
     * @param {Function} executor function to call to execute the task
     * @param {Function} catcher function to call if execution errors
     * @param {Function} finallyBlock function to call after the task was executed
     */
    constructor(consumer, groupId, message, executor, catcher, finallyBlock) {
        this._consumer = consumer;
        this._groupId = groupId;
        this._message = message;
        this._executor = executor;
        this.catch = catcher;
        this.finally = finallyBlock;
    }

    /**
     * A consumer which the message was emitted by
     *
     * @returns {HighLevelConsumer}
     */
    get consumer() {
        return this._consumer;
    }

    /**
     * A topic which the message belongs to
     * @returns {string}
     */
    get topic() {
        return this._message.topic;
    }

    /**
     * A partition which the message belongs to
     *
     * @returns {number}
     */
    get partition() {
        return this._message.partition;
    }

    /**
     * An offset of the message which created this task
     *
     * @returns {number}
     */
    get offset() {
        return this._message.offset;
    }

    /**
     * A consumer group which the task belongs to
     *
     * @returns {string}
     */
    get consumerGroup() {
        return this._groupId;
    }

    /**
     * Executes the task
     */
    exec() {
        return this._executor();
    }

    /**
     * Checks whether other task was for the same consumerGroup, topic and partition as this one.
     *
     * @param {Task} otherTask
     * @returns {boolean}
     */
    isTaskForSamePartition(otherTask) {
        return otherTask && otherTask.consumerGroup === this.consumerGroup
            && otherTask.topic === this.topic
            && otherTask.partition === this.partition;
    }
}

module.exports = Task;
