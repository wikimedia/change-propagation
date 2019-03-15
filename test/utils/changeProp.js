'use strict';

const ServiceRunner = require('service-runner');
const fs        = require('fs');
const yaml      = require('js-yaml');
const P         = require('bluebird');
const common    = require('./common');

const CHANGE_PROP_STOP_DELAY = 1000;
const CHANGE_PROP_START_DELAY = 15000;

let startupRetryLimit = 3;

var ChangeProp = function(configPath) {
    this._configPath = configPath;
    this._config = this._loadConfig();
    this._config.num_workers = 0;
    this._config.logging = {
        name: 'change-prop',
        level: 'fatal',
        streams: [{ type: 'stdout'}]
    };
    this._runner = new ServiceRunner();
    this._running = false;
};

ChangeProp.prototype._loadConfig = function() {
    return yaml.safeLoad(fs.readFileSync(this._configPath).toString());
};

ChangeProp.prototype.start = function() {
    if (this._running) {
        console.log('The test server is already running. Skipping start.')
        return P.resolve();
    }

    return this._runner.start(this._config)
    .tap(() => this._running = true)
    .delay(process.env.MOCK_KAFKA ? 0 : CHANGE_PROP_START_DELAY)
    .catch((e) => {
        if (startupRetryLimit > 0 && /EADDRINUSE/.test(e.message)) {
            console.log('Execution of the previous test might have not finished yet. Retry startup');
            startupRetryLimit--;
            return P.delay(1000).then(() => this.start());
        }
        throw e;
    });
};

ChangeProp.prototype.stop = function() {
    if (this._running) {
        return this._runner.stop()
        .tap(() => {
            common.clearKafkaFactory();
            this._running = false;
        })
        .delay(process.env.MOCK_KAFKA ? 0 : CHANGE_PROP_STOP_DELAY);
    }
    return P.resolve();
};

module.exports = ChangeProp;
