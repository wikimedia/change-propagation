"use strict";

const P = require('bluebird');
const Router = require('hyperswitch').Router;

class MetricsReporter {
    constructor(options) {
        this._options = options;
        this._router_cache = new Map();
    }

    _getRouter(hyper, domain) {
        if (!this._router_cache.has(domain)) {
            this._router_cache.set(domain, hyper.get({
                uri: `http://localhost:7231/${domain}/v1/?spec`
            })
            .then((res) => {
                const router = new Router({
                    appBasePath: '/api/rest_v1'
                });
                return router.loadSpec(res.body, hyper);
            }));
        }
        return this._router_cache.get(domain);
    }

    webrequest(hyper, req) {
        const message = req.body;
        return this._getRouter(hyper, message.uri_host)
        .then((router) => {
            const handler = router.route(message.uri_path.replace(/^\/api\/rest_v1/, ''));
            console.log(handler.value.path);
        })
        .thenReturn({ status: 200 });
    }
}

module.exports = (options) => {
    const processor = new MetricsReporter(options);
    return {
        spec: {
            paths: {
                '/webrequest': {
                    post: {
                        summary: 'report a request REST API',
                        operationId: 'webrequest'
                    }
                }
            }
        },
        operations: {
            webrequest: processor.webrequest.bind(processor)
        }
    };
};
