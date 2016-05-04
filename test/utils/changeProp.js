'use strict';

var ServiceRunner = require('service-runner');
var fs        = require('fs');
var yaml      = require('js-yaml');
var P         = require('bluebird');

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
};

ChangeProp.prototype._loadConfig = function() {
    return yaml.safeLoad(fs.readFileSync(this._configPath).toString());
};

ChangeProp.prototype.start = function() {
    var self = this;
    self.port = self._config.services[0].conf.port;
    self.hostPort = 'http://localhost:' + self.port;
    return self._runner.run(self._config)
    .then(function(servers) {
        self._servers = servers;
        return true;
    });
};

ChangeProp.prototype.stop = function() {
    var self = this;
    if (self._servers) {
        return P.each(self._servers, function(server) {
            return server.close();
        })
        .then(function() {
            self._servers = undefined;
        })
        .delay(100);
    } else {
        return P.delay(100);
    }
};

module.exports = ChangeProp;
