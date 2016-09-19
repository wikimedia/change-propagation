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
    return prevTrigger + event.meta.topic + ':' + event.meta.uri;
};

utils.augmentRequest = (request, event) => {
    request.headers = Object.assign(request.headers || {}, {
        'x-request-id': event.meta.request_id,
        'x-triggered-by': utils.triggeredBy(event)
    });
    return request;
};

module.exports = utils;
