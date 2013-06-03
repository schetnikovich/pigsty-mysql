var debug = require('debug')('pigsty-mysql')
var PigstyPlugin = require('pigsty-plugin');
var Insert = require('./insert');
var database_session= require('./query');
var Sensors = require('./sensors');
var Signatures = require('./signatures');

MysqlPlugin.prototype = new PigstyPlugin();
MysqlPlugin.prototype.constructor = PigstyPlugin;

function MysqlPlugin(options) {
  var self = this;
  PigstyPlugin.call(this, options);
  self.options = options;
  self.count = 0;
  self.pending = 0;
  self.queue = [];
};


MysqlPlugin.prototype.start = function(callback) {
  var self = this;

  database_session(self.options, function(err, db) {
    self.db = db;
    self.sensors = new Sensors({ db: self.db });
    self.signatures = new Signatures({ db: self.db });
    self.start_time = new Date().getTime();
    
    self.emit('ready');
  });

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
 
  self.pending += 1;

  // tell the parser we are full if we get > 
  // 2000 events in the queue.
  if (self.pending > 2000 && !self.paused) {
    self.emit('full');
    self.paused = setInterval(function() {
      if (self.pending < 1500 && self.paused) {
        clearInterval(self.paused);
        self.paused = null;
        self.emit('ok');
      }
    }, 500);
  }

  database_session(self.options, function(err, db) {
    self.pending -= 1;

    var event_inserter = new Insert({
      db: db,
      event: event,
      sensors: self.sensors,
      signatures: self.signatures,
      encoding: self.options.encoding,
      localtime: self.options.localtime
    });

    event_inserter.once('error', function(err) {
      console.error('unable to insert:', event, err);
      // console.error("plugin error:",);
      self.emit('error', { message: err, event: event });
    });

    event_inserter.once('end', function() {
      self.count += 1;
      if (self.options.print_statistics) {
        if (self.count % 100 == 0) {
          var rate = self.count / ((new Date().getTime() - self.start_time) / 1000);
          console.log('mysql processed count:', self.count, "rate (eps):", rate);
        }
      }
      db.stop();
    });

    event_inserter.run(); 
  })
 
};

module.exports = function(options) {
  return new MysqlPlugin(options);
};
