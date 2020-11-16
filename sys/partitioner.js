'use strict';

const extend = require('extend');
const Template = require('hyperswitch').Template;

class Partitioner {
    constructor(options) {
        this._options = options || {};

        if (!options.templates || !options.templates.partition_stream) {
            throw new Error('No partition_stream template was provided to the partitioner');
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

        this._partitionedStreamTemplate = new Template(options.templates.partition_stream);
    }

    /**
     * Returns a partition key value based on a configured partition key
     *
     * @param {Object} event
     * @return {undefined|string}
     * @private
     */
    _getPartitionKeyValue(event) {
        const result = this._options.partition_key.split('.')
        .reduce((value, key) => value ? value[key] : undefined, event);
        return typeof result === 'string' ? result : undefined;
    }

    /**
     * Selects a proper partition and reposts the message to the partitioned topic.
     *
     * @param {HyperSwitch} hyper
     * @param {Object} req
     * @return {Object}
     */
    repostToPartition(hyper, req) {
        const origEvent = req.body;
        let partition = this._options.partition_map[this._getPartitionKeyValue(origEvent)];
        if (partition === undefined) {
            partition = this._options.partition_default;
        }

        // Clone the event and meta sub-object to avoid modifying
        // the original event since that could mess up processing
        // in the executor regarding metrics, limiters, follow-up
        // executions etc.
        const event = extend(true, {}, origEvent);
        event.meta.stream = this._partitionedStreamTemplate.expand({
            message: event
        });
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
