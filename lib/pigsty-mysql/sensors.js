var debug = require('debug')('pigsty')

function Sensor(options) {
  var self = this;
  options = options || {}
  self.db = options.db;
  self.sid = 0;
  self.cid = 0;
};

Sensor.prototype._max_cid = function(sensor, callback) {
  var self = this;

  var query = 'select max(cid) as cid from event where \
  sid = ?';
  
  var params = [ self.sid ];

  var q = self.db(query);
  
  q.selectOne(params, function(err, result) {

    if (err) {
      return callback(err); 
    }
 
    if (result && result.cid) {
      self.cid = result.cid;
    };
    debug('using max cid:', self.cid);
    callback(null);
    // TODO: update cid in sensor table;
  });
};

Sensor.prototype.increment_cid = function() {
  var self = this;
  self.cid += 1;
  return self.cid;
}


Sensor.prototype.lookup = function(sensor, callback) {
  var self = this;

  if (self.err) {
    return callback(err);
  };

  if (!sensor) {
    return callback("No sensor provided");
  };

  if (!sensor.name) {
    return callback("No sensor name provided");
  }

  sensor.interface = sensor.interface || "";
  sensor.filter = sensor.filter || null;
  sensor.name = sensor.name || ""; 
  sensor.encoding = sensor.encoding || null;
  sensor.detail = sensor.detail || null;

  var query = 'select sid from sensor where \
  hostname = ? and interface = COALESCE(?, interface) \
  and COALESCE(filter, -1) = COALESCE(?, filter, -1) \
  and COALESCE(encoding, -1) = COALESCE(?, encoding, -1) \
  and COALESCE(detail, -1) = COALESCE(?, detail, -1) \
  LIMIT 1';
  
  var params = [ sensor.name, sensor.interface, sensor.filter,
  sensor.encoding, sensor.detail ];

  var find_sensor =  self.db(query);

  find_sensor.selectOne(params, function(err, sensor) {

    if (err) {
      return callback(err); 
    }

    if (sensor) {
      self.err = null;
      self.sid = sensor.sid; 
      return callback(null, sensor) ;
    }
    // otherwise, insert
    var query = 'insert into sensor (hostname, interface, \
    filter, encoding, detail) values (?,?,?,?,?)';
    var insert = self.db(query);
  
    insert.execute(params, function(err, result) {
        self.sid = result.insert_id;
        callback(err, result);
    }); 
  });
}

Sensor.prototype.load = function(sensor, callback) {
  var self = this;
  self.lookup(sensor, function(err) {
    if (err) {
      return callback(err); 
    }
    self._max_cid(sensor, function(err) {
      if (err) {
        return callback(err); 
      }
      callback(null);
    });
  });
};


function Sensors(options) {
  var self = this;
  self.db = options.db;
  self.locked = false; // ghettolock 
  self.sensors = {};
};

Sensors.prototype.lookup = function(sensor, callback) {
  var self = this;
  var id = sensor.id;  

  if (self.sensors[id]) {
    return callback(null, self.sensors[id]);
  };
 
  if (self.locked) {
    // wait a couple of secs
    return setTimeout(function() {
      debug('sensors lock busy...');
      self.lookup(sensor, callback); 
    }, 1000) 
  }

  self.locked = true;

  var s = new Sensor({ db: self.db });
  
  s.load(sensor, function(err) {
    self.locked = false;
    if (err) {
      return callback(err); 
    } else {
      self.sensors[id] = s;
      return callback(null, self.sensors[id]);
    } 
  })
};




module.exports = Sensors;
// var db = require('./query')();
// var s = new Sensor({ db: db });
// s.load({
 // hostname: "jen_laptop",
 // interface: 'en1'
// }, function(err, data) {
  // console.log("got data: ", err, data);

// });



