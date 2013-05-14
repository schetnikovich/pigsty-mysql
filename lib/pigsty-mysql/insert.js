var debug = require('debug')('pigsty')
var PigstyPlugin = require('pigsty-plugin');
var EventEmitter = require('events').EventEmitter;


Insert.prototype = new EventEmitter();
Insert.prototype.constructor = Insert;

function Insert(options) {
  var self = this;
  self.db = options.db;
  self.sensors = options.sensors;
  self.event = options.event
};


Insert.prototype._prepare = function(sensor) {
  var self = this;
  var event = self.event;

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
      return self.emit('error', "No sensor: " +  err);
    }

    console.log("XXX: self.event:", self.event);

  });

  // self.sensors.lookup(self.event.sensor, function(sensor) {
  
  // });
};

module.exports = Insert;



