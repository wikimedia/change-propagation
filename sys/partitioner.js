'use strict';

const extend = require('extend');
const Template = require('hyperswitch').Template;

class Partitioner {
    constructor(options) {
        this._options = options || {};

        if (!options.templates || !options.templates.partition_topic) {
            throw new Error('No partition_topic template was provided to the partitioner');
        }

        if (!options.partition_key) {
            throw new Error('No partition_key was provided to the partitioner');
        }

        if (!options.partition_map) {
            throw new Error('No partition_map was provided to the partitioner');
        }

        if (!options.partition_default) {
            throw new Error('No partition_default was provided to the partitioner');
        }

        this._partitionedTopicTemplate = new Template(options.templates.partition_topic);
    }

    /**
     * Selects a proper partition and reposts the message to the partitioned topic.
     * @param {HyperSwitch} hyper
     * @param {Object} req
     * @return {Object}
     */
    repostToPartition(hyper, req) {
        const origEvent = req.body;
        const partitionKeyValue = origEvent[this._options.partition_key];
        let partition = this._options.partition_map[partitionKeyValue];
        if (partition === undefined) {
            partition = this._options.partition_default;
        }

        // Clone the event and meta sub-object to avoid modifying
        // the original event since that could mess up processing
        // in the executor regarding metrics, limiters, follow-up
        // executions etc.
        const event = extend(true, {}, origEvent);
        // TODO: Temporary workaround for eventgate transition.
        // Support both meta.topic and meta.stream
        event.meta.topic = event.meta.stream = event.meta.topic || event.meta.stream;
        const partitionedTopic = this._partitionedTopicTemplate.expand({
            message: event
        });
        event.meta.topic = event.meta.stream = partitionedTopic;
        // Bring it back to the style that the original event had.
        if (!origEvent.meta.topic) {
            delete event.meta.topic;
        }
        if (!origEvent.meta.stream) {
            delete event.meta.stream;
        }
        return hyper.post({
            uri: `/sys/queue/events/${partition}`,
            body: [ event ]
        });
    }
}

module.exports = (options) => {
    const ps = new Partitioner(options);

    return {
        spec: {
            paths: {
                '/': {
                    post: {
                        operationId: 'repostToPartition'
                    }
                }
            }
        },
        operations: {
            repostToPartition: ps.repostToPartition.bind(ps)
        }
    };
};
