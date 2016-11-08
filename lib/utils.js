"use strict";

const utils = {};

/**
 * Computes the x-triggered-by header
 *
 * @param {Object} event the event
 * @returns {string}
 */
utils.triggeredBy = (event) => {
    let prevTrigger = event.triggered_by || '';
    if (prevTrigger) {
        prevTrigger += ',';
    }
    return `${prevTrigger + event.meta.topic}:${event.meta.uri}`;
};

/**
 * Safely stringifies the event to JSON string.
 *
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
