"use strict";

const DEDUPE_LOG_SIZE = 100;
const latestIDs = {};
const latestRootEvents = {};

const isDuplicate = (name, message) => {
    // First, look at the individual event duplication
    if (latestIDs[name].indexOf(message.meta.id) > -1) {
        // This individual message already was processed by this rule
        return true;
    }
    latestIDs[name].push(message.meta.id);
    if (latestIDs[name].length > DEDUPE_LOG_SIZE) {
        latestIDs[name] = latestIDs[name].slice(1);
    }

    // Now look at the root event deduplication
    if (message.root_event) {
        if (latestRootEvents[name].some((oldRootEvent) => {
            return oldRootEvent.signature === message.root_event.signature
                    && new Date(oldRootEvent.dt) > new Date(message.root_event.dt);
        })) {
            // Deduplicate based on a root event
            return true;
        }

        latestRootEvents[name].push(message.root_event);
        if (latestRootEvents[name].length > DEDUPE_LOG_SIZE) {
            latestRootEvents[name] = latestRootEvents[name].slice(1);
        }

    }
    return false;
};

module.exports = (hyper, req, next, options) => {
    try {

        const filterName = options.name || 'default_deduplicator';
        latestIDs[filterName] = latestIDs[filterName] || [];
        latestRootEvents[filterName] = latestRootEvents[filterName] || [];

        if (!isDuplicate(filterName, req.body)) {
            return next(hyper, req);
        }

        hyper.metrics.increment(`${filterName}_deduplicate`);
        // Skip if duplication detected
        return { status: 200 };
    } catch (e) {
        hyper.log('error/dedupe', {
            message: 'Error in deduplication',
            error: e
        });
    }
};
