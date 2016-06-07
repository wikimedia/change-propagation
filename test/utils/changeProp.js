'use strict';

var ServiceRunner = require('service-runner');
var fs        = require('fs');
var yaml      = require('js-yaml');
var P         = require('bluebird');

const CHANGE_PROP_STOP_DELAY = 500;

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
    .delay(200)
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
        .tap(() => this._running = false)
        .delay(CHANGE_PROP_STOP_DELAY);
    }
};

module.exports = ChangeProp;
