var debug = require('debug')('pigsty-mysql')
var PigstyPlugin = require('pigsty-plugin');
var EventEmitter = require('events').EventEmitter;


Insert.prototype = new EventEmitter();
Insert.prototype.constructor = Insert;

function Insert(options) {
  var self = this;
  self.db = options.db;
  self.sensors = options.sensors;
  self.signatures = options.signatures;
  self.event = options.event;
};


Insert.prototype._signature = function(event, callback) {
  var self = this;

  self.signatures.lookup(event, function(err, sig_id) {
    if (err) {
      callback(err);
    }
    self.sig_id = sig_id;
    callback();
  })
};

Insert.prototype.run = function() {
  var self = this;
  var event = self.event;

  if (!event || Object.keys(self.event) == 0) {
    return; 
  };

  if (!event.sensor) {
    return self.emit('error', { msg: "No sensor for event: " , event: event });
  };

  if (!event.event_type) {
    return self.emit('error', "No event type for event: " + event);
  };

  self.sensors.lookup(event.sensor, function(err, sensor) {

    if (err) {
      return self.emit('error', err);
    }

    self.sid = sensor.sid;
    self.cid = sensor.increment_cid();

    self._signature(event.event, function(err) {

      if (err) {
        return self.emit('error', err);
      }
    })

  });

};

module.exports = Insert;



