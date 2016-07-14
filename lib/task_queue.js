'use strict';


const EventEmitter = require('events');
const P = require('bluebird');
const kafka = require('librdkafka-node');


/**
 * The default queue size
 *
 * @type {number}
 * @const
 */
const DEFAULT_SIZE = 100;

/**
 * The default maximum delay to commit the offsets
 *
 * @const
 * @type {number}
 */
const DEFAULT_COMMIT_INTERVAL = 500;


/**
 * A class representing the task queue used in processing events.
 */
class TaskQueue extends EventEmitter {

    /**
     * Creates a new task queue instance
     *
     * @param {Object} [options={}]
     * @param {number} [options.size=DEFAULT_SIZE] the maximum number of tasks in the queue
     * @param {number} [options.commit_interval=DEFAULT_COMMIT_INTERVAL] the maximum delay
     *                                                                   to commit the offsets
     * @constructor
     */
    constructor(options) {
        super();
        options = options || {};
        this._max_size = options.size || DEFAULT_SIZE;
        this._commitInterval = options.commit_interval || DEFAULT_COMMIT_INTERVAL;
        this._queue = [];
        this._waiting = [];
        this._consumers = [];
        this._commitScheduled = false;
        this._pendingCommits = new Map();

        this.on('task_completed', this._deferred.bind(this));
    }

    /**
     * Adds a consumer to the queue's list of consumers, which are used to pause
     * and resume them
     *
     * @param {HighLevelConsumer} consumer
     * @returns true if the consumer has been added, false if it has already been
     *               present in the list of consumers
     */
    addConsumer(consumer) {
        if (this._consumers.some((item) => item === consumer)) {
            return false;
        }
        this._consumers.push(consumer);
        return true;
    }

    /**
     * Enqueues a task on the queue. If the queue is full, the task will be placed on a waiting
     * list until enough tasks have been dequeued.
     *
     * @param {Task} task the task to enqueue
     * @param {HighLevelConsumer} task.consumer the Kafka consumer object used for offset commit
     * @param {Function} task.exec the promise to execute
     * @param {Function} task.catch the promise to execute in case the task's execution fails
     * @returns {Boolean} true if the task has been enqueued, false if it has been put on the
     *                    waiting list
     */
    enqueue(task) {
        if (this._queue.length >= this._max_size) {
            // too many tasks running already, defer this one
            this._defer(task);
            return false;
        }
        // ok, the execution can start right away
        this._start(task);
        return true;
    }

    /**
     * Finds the lowest offset that is safe to commit after some task finished.
     *
     * @param {Task} task just finished task
     * @private
     */
    _updatePendingCommits(task) {
        let consumerOffsets = this._pendingCommits.get(task.consumer);
        const current = consumerOffsets
            && consumerOffsets[task.topic]
            && consumerOffsets[task.topic][task.partition];
        const pendingPartitionTasks = this._queue
        .filter(task.isTaskForSamePartition.bind(task))
        .map((item) => item.offset);
        let newOffset = Math.min.apply(null, pendingPartitionTasks);
        if (newOffset === task.offset) {
            // In this case there's no pending tasks with an offset lower then
            // this finished task offset, so it's safe to move to the next offset.
            // If there were pending tasks with lower offsets, we couldn't have
            // incremented the offset since we don't know how if the pending task
            // fails or not.
            newOffset++;
        }
        if (current && current >= newOffset) {
            // A higher offset is already scheduled for commit
            return;
        }

        if (!consumerOffsets) {
            consumerOffsets = {};
            this._pendingCommits.set(task.consumer, consumerOffsets);
        }
        consumerOffsets[task.topic] = consumerOffsets[task.topic] || {};
        consumerOffsets[task.topic][task.partition] = newOffset;
    }

    /**
     * Begins the task execution
     *
     * @param {Task} task
     * @private
     */
    _start(task) {
        this._queue.push(task);
        task._p = P.try(() => {
            return task.exec().finally(() => {
                this._updatePendingCommits(task);
                this._queue = this._queue.filter((item) => item !== task);
                this.emit('task_completed');
            }).catch(task.catch).finally(task.finally);
        });
    }

    /**
     * Defers the task execution, puts it into the waiting queue
     *
     * @param {Task} task
     * @private
     */
    _defer(task) {
        // put the task in the waiting queue and pause the consumers
        this._waiting.push(task);
    }

    /**
     * Begins execution of the deferred tasks
     *
     * @private
     */
    _deferred() {
        this._commitOffsets();

        // enqueue any outstanding tasks
        while (this._queue.length < this._max_size && this._waiting.length > 0) {
            const task = this._waiting.shift();
            if (task.consumer.topicPayloads.some((p) => p.topic === task.topic)) {
                this._start(task);
            }
        }
    }

    _doCommit() {
        return P.each(this._pendingCommits.keys(), (consumer) => {
            const topicsOffsets = this._pendingCommits.get(consumer);
            return P.each(Object.keys(topicsOffsets), (topic) => {
                // TODO: more efficient
                const commit = new kafka.TopicPartition(topic, 0, topicsOffsets[topic]);
                consumer.commit([ commit ]);
            })
            .then(() => {
                this._pendingCommits.delete(consumer);
            });
        })
        .then(() => {
            this._commitScheduled = false;
        });
    }
    /**
     * Schedules a commit for pending commit requests
     *
     * @private
     */
    _commitOffsets() {
        if (this._commitScheduled) {
            return;
        }

        this._commitTimeout = setTimeout(this._doCommit.bind(this),
            this._commitInterval);
        this._commitScheduled = true;
    }
    
    close() {
        if (this._commitTimeout) {
            clearTimeout(this._commitTimeout);
            return this._doCommit();
        }
        return P.resolve();
    }
}


module.exports = TaskQueue;
