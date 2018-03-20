"use strict";

class Partitioner {
    constructor(options) {
        this._options = options || {};
        this._log = this._options.log || (() => {});

        if (!options.partition_topic_name) {
            throw new Error('No partition_topic_name was provided to the partitioner');
        }

        if (!options.partition_key) {
            throw new Error('No partition_key was provided to the partitioner');
        }

        if (!options.partition_map) {
            throw new Error('No partition_map was provided to the partitioner');
        }
    }

    /**
     * Selects a proper partition and reposts the message to the partitioned topic.
     * @param {HyperSwitch} hyper
     * @param {Object} req
     */
    repostToPartition(hyper, req) {
        const event = req.body;
        const partitionKeyValue = event[this._options.partition_key];
        let partition = this._options.partition_map[partitionKeyValue];
        if (partition === undefined) {
            partition = this._options.partition_default;
        }

        event.meta.topic = this._options.partition_topic_name;
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
