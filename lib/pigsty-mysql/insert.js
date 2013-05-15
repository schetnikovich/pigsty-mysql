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

/**
 *INSERT INTO "
*"iphdr (sid, cid, ip_src, ip_dst, ip_ver, ip_hlen, "
*"       ip_tos, ip_len, ip_id, ip_flags, ip_off,"
*"       ip_ttl, ip_proto, ip_csum) "
*"VALUES (%u,%u,%lu,%lu,%u,%u,%u,%u,%u,%u,%u,%u,%u,%u)",
*
 *
 *
 */
Insert.prototype._add_ip = function(callback) {
  var self = this;

  if (!self.event.packets || !self.event.packets.length > 0) {
    debug('no ip hdr');
    return callback("Missing ip packet");
  }
  var packet = self.event.packets[0].packet;
  debug('packet', packet);

  return;

  var query = 'insert into iphdr  (sid, cid, ip_sport, ip_dport, ' +
  'ip_seq, ip_ack, ip_off, ip_res, ' +
  'ip_flags, ip_win, ip_csum, ip_urp) ' +
  'VALUES (?,?,?,?,?,?,?,?,?,?,?,?)';
  
  var q = self.db(query);
  var event = self.event.event;
  var params = [self.sid, self.cid, event.source_port, event.dest_port,
  
  ]
  q.execute(params, function(err, result) {
    
    if (err) {
      callback(err)
    };
    callback(null);
  });
};




Insert.prototype._add_tcp = function(event, callback) {
  var self = this;
  var query = 'insert into tcphdr  (sid, cid, tcp_sport, tcp_dport, ' +
  'tcp_seq, tcp_ack, tcp_off, tcp_res, ' +
  'tcp_flags, tcp_win, tcp_csum, tcp_urp) ' +
  'VALUES (?,?,?,?,?,?,?,?,?,?,?,?)';
  
  var q = self.db(query);
  var params = [self.sid, self.cid, self.source_port, self.dest_port,
  
  ]
  q.execute(params, function(err, result) {
    
    if (err) {
      callback(err)
    };
    callback(null);
  });
};


Insert.prototype._add_event = function(event, callback) {
  var self = this;
  var query = 'insert into event (sid, cid, signature, timestamp)\
  values (?, ?, ?, FROM_UNIXTIME(?))';
  var q = self.db(query);

  q.execute([self.sid, self.cid, self.sig_id, event.event_second], function(err, result) {
    
    if (err) {
      callback(err)
    };
    callback(null);
  });
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
    debug('missing event type: ', event);
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
      self._add_event(event.event, function(err) {
        debug('adding event: ', sensor, self.event);
        if (err) {
          return self.emit('error', err);
        }

      })
   
    })
  });
};

module.exports = Insert;



