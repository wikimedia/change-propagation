"use strict";

const P = require('bluebird');
const HyperSwitch = require('hyperswitch');
const stringify = require('json-stable-stringify');
const Router = HyperSwitch.Router;
const Template = HyperSwitch.Template;

class MetricsReporter {
    constructor(options) {
        this._options = options || {};
        this._options.sample_rate = this._options.sample_rate || 0.01;
        this._specRequestTemplate = new Template(options.templates.spec_request);

        this._router_cache = new Map();
        this._spec_cache = new Map();
    }

    _getRouter(hyper, message) {
        const domain = message.uri_host;
        if (!this._router_cache.has(domain)) {
            return hyper.get(this._specRequestTemplate.expand({ message }))
            .then((res) => {
                const spec = res.body;
                const stringSpec = stringify(spec);

                let sameSpecDomain;
                for (const entry of this._spec_cache.entries()) {
                    if (entry[1] === stringSpec) {
                        sameSpecDomain = entry[0];
                        break;
                    }
                }

                let router;

                if (sameSpecDomain) {
                    router = this._router_cache.get(sameSpecDomain);
                    this._router_cache.set(domain, router);
                } else {
                    this._spec_cache.set(domain, stringSpec);
                    router = new Router({
                        appBasePath: '/api/rest_v1',
                        disable_handlers: true
                    });
                    router = router.loadSpec(spec, hyper);
                    this._router_cache.set(domain, P.resolve(router));
                }
                return router;
            });
        }
        return this._router_cache.get(domain);
    }

    webrequest(hyper, req) {
        const message = req.body;
        return this._getRouter(hyper, message)
        .then((router) => {
            const handler = router.route(message.uri_path.replace(/^\/api\/rest_v1/, ''));
            const path = handler.value.path;
            let statName = `${path}.${message.http_method.toUpperCase()}.`;
            // Normalize invalid chars
            statName = hyper.metrics.normalizeName(statName);
            hyper.metrics.increment(statName, 1, this._options.sample_rate);
        })
        .catch((e) => {
            // Ignore all errors, don't want to die or retry
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
