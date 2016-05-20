"use strict";

const utils = {};

/**
 * Computes the x-triggered-by header
 *
 * @param {Object} event the event
 * @returns {string}
 */
utils.triggeredBy = (event) => {
    return event.triggered_by ?
    event.triggered_by + ',' + event.meta.topic + ':' + event.meta.uri :
    event.meta.topic + ':' + event.meta.uri;
};

module.exports = utils;
