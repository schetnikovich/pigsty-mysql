var debug = require('debug')('pigsty-mysql')
var PigstyPlugin = require('pigsty-plugin');
var Insert = require('./insert');
var db = require('./query');
var Sensors = require('./sensors');
var Signatures = require('./signatures');

MysqlPlugin.prototype = new PigstyPlugin();
MysqlPlugin.prototype.constructor = PigstyPlugin;

function MysqlPlugin(options) {
  var self = this;
  PigstyPlugin.call(this, options);
  self.options = options;
  self.count = 0;
};

MysqlPlugin.prototype.configure = function(callback) {
  debug('configuring pigsty-mysql');
};

MysqlPlugin.prototype.start = function(callback) {
  var self = this;
  self.db = db(self.options);
  self.sensors = new Sensors({ db: self.db });
  self.signatures = new Signatures({ db: self.db });
  self.start_time = new Date().getTime();
};

MysqlPlugin.prototype.stop = function(callback) {
  var self = this;
  // TODO: stop db?
  if (self.db) {
    self.db.stop();
    self.db = null;
  }
};

MysqlPlugin.prototype.send = function(event) {
  var self = this;
  var event_inserter = new Insert({
    db: self.db,
    event: event,
    sensors: self.sensors,
    signatures: self.signatures,
  });

  event_inserter.on('error', function(err) {
    debug('unable to insert:', event);
    console.error("plugin error:", err);
    self.emit('error', err);
  });

  event_inserter.on('end', function() {
    self.count += 1;
    if (self.count % 100 == 0) {
      var rate = self.count / ((new Date().getTime() - self.start_time) / 1000);
      console.log('processed count:', self.count, rate);
    }
  })

  event_inserter.run();
};

module.exports = function(options) {
  return new MysqlPlugin(options);
};
