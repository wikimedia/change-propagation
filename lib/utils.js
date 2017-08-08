"use strict";

const TimeUUID = require('cassandra-uuid').TimeUuid;

const utils = {};

/**
 * Computes the x-triggered-by header
 * @param {Object} event the event
 * @return {string}
 */
utils.triggeredBy = (event) => {
    let prevTrigger = event.triggered_by || '';
    if (prevTrigger) {
        prevTrigger += ',';
    } else {
        prevTrigger = `req:${event.meta.request_id},`;
    }
    return `${prevTrigger + event.meta.topic}:${event.meta.uri}`;
};

utils.requestId = () => TimeUUID.now().toString();
/**
 * Safely stringifies the event to JSON string.
 * @param {Object} event the event to stringify
 * @return {string|undefined} stringified event or undefined if failed.
 */
utils.stringify = (event) => {
    try {
        return JSON.stringify(event);
    } catch (e) {
        return undefined;
    }
};

module.exports = utils;
