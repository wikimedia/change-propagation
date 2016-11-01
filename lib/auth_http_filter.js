"use strict";

const utils = require('./utils');
const Template = require('hyperswitch').Template;
const P = require('bluebird');

const processSetCookie = (setCookieHeader) => {
    return setCookieHeader.map((item) => item.substr(0, item.indexOf(';'))).join('; ');
};

const loginToMediawiki = (hyper, req, next, options) => {
    // We're calling 'next', not 'hyper.post' to bypass this filter.
    return next(hyper, options._cache.tokenRequestTemplate.expand({ request: req }))
    .then((res) => {
        const loginToken = res.body.query.tokens.logintoken;
        const mwApiLoginReq = options._cache.loginRequestTemplate.expand({ request: req });
        mwApiLoginReq.headers.cookie = processSetCookie(res.headers['set-cookie']);
        mwApiLoginReq.body.lgtoken = loginToken;

        // We're calling 'next', not 'hyper.post' to bypass this filter.
        return next(hyper, mwApiLoginReq);
    })
    .then((res) => {
        options._cache[req.params.domain] = processSetCookie(res.headers['set-cookie']);
        return options._cache[req.params.domain];
    });
};

const getCookie = (hyper, req, next, options) => {
    if (options._cache[req.params.domain]) {
        return P.resolve(options._cache[req.params.domain]);
    } else {
        return loginToMediawiki(hyper, req, next, options);
    }
};

module.exports = function(hyper, req, next, options) {
    options = options || {};
    options._cache = options._cache || {};
    options._cache.tokenRequestTemplate = options._cache.tokenRequestTemplate ||
        new Template(Object.assign(options.templates.mw_api, {
            body: {
                action: 'query',
                meta: 'tokens',
                type: 'login',
                formatversion: 2,
                format: 'json'
            }
        }));
    options._cache.loginRequestTemplate = options._cache.loginRequestTemplate ||
        new Template(Object.assign(options.templates.mw_api, {
            body: {
                action: 'login',
                lgname: options.username,
                lgpassword: options.password,
                formatversion: 2,
                format: 'json'
            }
        }));

    const host = req.uri.constructor === String ? req.uri : req.uri.protoHost;
    if (!options._cache.authenticateRegex) {
        options._cache.authenticateRegex = utils.constructRegex(options.hosts);
    }

    if (!options._cache.authenticateRegex.test(host)) {
        return next(hyper, req);
    }

    return getCookie(hyper, req, next, options)
    .then((cookie) => {
        req.headers.cookie = cookie;
        return next(hyper, req);
        // TODO: Make this handle logouts by session expiration
        // TODO: Make it handle wrong credentials
    });
};