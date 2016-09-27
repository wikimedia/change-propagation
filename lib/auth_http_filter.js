"use strict";

const utils = require('./utils');

const getCookie = (hyper, options) => {
    if (options._cache.cookie) {
        return options._cache.cookie;
    }

    return hyper.post({
        uri: options.mw_api_uri,
        body: {
            action: 'query',
            meta: 'tokens',
            type: 'login',
            format: 'json',
            formatversion: 2
        }
    })
    .then((res) => {
        console.log('RESULT', res.body);
    });
};

module.exports = function(hyper, req, next, options) {
    console.log('AAAA', req);
    options = options || {};
    const host = req.uri.constructor === String ? req.uri : req.uri.protoHost;

    if (!options._cache.authenticateRegex) {
        options._cache.authenticateRegex = utils.constructRegex(options.hosts);
    }

    console.log(host);
    if (options._cache.authenticateRegex.test(host)) {
        console.log('DONE');
        return getCookie(hyper, options)
        .then(() => next(hyper, req));
    }

    return next(hyper, req);
};