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
 * From a list of uri Regex and values, constructs a regex to check if the
 * request URI is in the white-list.
 */
utils.constructRegex = (variants) => {
    let regex = (variants || []).map((regexString) => {
        regexString = regexString.trim();
        if (/^\/.+\/$/.test(regexString)) {
            return `(:?${regexString.substring(1, regexString.length - 1)})`;
        } else {
            // Instead of comparing strings
            return `(:?^${regexString.replace(/[\-\[\]\/\{}\(\)\*\+\?\.\\\^\$\|]/g, "\\$&")})`;
        }
    }).join('|');
    regex = regex && regex.length > 0 ? new RegExp(regex) : undefined;
    return regex;
};

module.exports = utils;
