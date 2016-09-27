"use strict";

const utils = require('./utils');

const processSetCookie = (setCookieHeader) => {
    return setCookieHeader.map((item) => item.substr(0, item.indexOf(';'))).join('; ');
};

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
            formatversion: 2,
            format: 'json'
        }
    })
    .then((res) => {
        console.log('FIRST!!!', JSON.stringify(res));
        const loginToken = res.body.query.tokens.logintoken;
        return hyper.post({
            uri: options.mw_api_uri,
            headers: {
                cookie: processSetCookie(res.headers['set-cookie'])
            },
            body: {
                action: 'login',
                lgname: 'Pchelolo@Pchelolo_Test_Bot',
                lgpassword: 'h0i1aonogt4q6linfo2431clc724gbdr',
                lgtoken: loginToken,
                formatversion: 2,
                format: 'json'
            }
        })
    })
    .then((res) => {
        console.log('OVERALL RESULT', JSON.stringify(res));
        return hyper.get({
            uri: 'https://en.wikipedia.org/w/api.php?action=query&meta=userinfo&uiprop=rights%7Chasmsg',
            headers: {
                cookie: processSetCookie(res.headers['set-cookie'])
            },
            query: {
                format: 'json',
                formatversion: 2
            }
        });
    })
    .then((res) => {
        console.log(res.body);
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