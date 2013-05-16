var debug = require('debug')('pigsty-mysql')
var PigstyPlugin = require('pigsty-plugin');
var EventEmitter = require('events').EventEmitter;


Insert.prototype = new EventEmitter();
Insert.prototype.constructor = Insert;

function tcpflags(flags) {
  var val = 0;
  var pos = 0;
  for (var k in flags) {
    var flag = flags[k];
    if (flag) {
      val = val | (1 << pos);
    }
    pos++;
  };
  return val;
};


function Insert(options) {
  var self = this;
  self.db = options.db;
  self.encoding = options.encoding || 'hex';
  self.sensors = options.sensors;
  self.signatures = options.signatures;
  self.event = options.event;
};

Insert.prototype._add_ip_opt = function(opt, callback) {
  var self = this;

  // XXX: TODO
  var query = "INSERT INTO " +
  "opt (sid,cid,optid,opt_proto,opt_code,opt_len,opt_data) " +
   "VALUES (?,?,?,?,?,?,?)";

  callback(null);
};

Insert.prototype._add_payload = function(callback) {
  
  var self = this;

  if (self.event.packets && self.event.packets.length > 0) {
    var data = self.event.packets[0].bytes;

    // TODO: other encodings
    //
    if (self.encoding == 'base64' || self.encoding == 'hex') {
      var data = data.toString(self.encoding).toUpperCase();
      var query = "INSERT INTO " +
      "data (sid,cid,data_payload) " +
      "VALUES (?,?,?)";

      var q = self.db(query);
 
      var params = [self.sid, self.cid, data]; 
      
      q.execute(params, function(err, result) {

        if (err) {
          return callback(err);
        };

        callback(null);
      });


    } else {
      console.error("unsupported encoding: ", self.encoding);
      callback();
    }
 
  } else {
    return callback();
  }
};


Insert.prototype._add_icmp = function(packet, callback) {
  
  var self = this;

  if (!packet.protocol_name == "ICMP" || !packet.ip.icmp) {
    return callback(); 
  }

  var query = "INSERT INTO " +
  "icmphdr (sid, cid, icmp_type, icmp_code, icmp_csum, icmp_id, icmp_seq) " +
  "VALUES (?,?,?,?,?,?,?)";

  var q = self.db(query);
  var event = self.event.event;
  
  var params = [self.sid, self.cid, packet.ip.icmp.type,
    packet.ip.icmp.code, packet.ip.icmp.checksum, packet.ip.icmp.id,
    packet.ip.icmp.sequence];

  q.execute(params, function(err, result) {
 
    if (err) {
      return callback(err);
    };
   
    callback(null);
  });
};


Insert.prototype._add_tcp = function(packet, callback) {
  var self = this;

  if (!packet.protocol_name == "TCP" || !packet.ip.tcp) {
    return callback(); 
  }

  var query = "insert into tcphdr (sid, cid, tcp_sport, tcp_dport, " +
  "        tcp_seq, tcp_ack, tcp_off, tcp_res, " +
  "        tcp_flags, tcp_win, tcp_csum, tcp_urp) " +
  "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)";

  var q = self.db(query);
  var event = self.event.event;
  
  var params = [self.sid, self.cid, packet.ip.tcp.sport, packet.ip.tcp.dport, 
    packet.ip.tcp.seqno, packet.ip.tcp.ackno, packet.ip.tcp.data_offset,
    packet.ip.tcp.reserved,
    tcpflags(packet.ip.tcp.flags),
    packet.ip.tcp.window_size, 
    packet.ip.tcp.checksum,
    packet.ip.tcp.urgent_pointer,
  ];

  q.execute(params, function(err, result) {
    
    if (err) {
      return callback(err);
    };
   
    callback(null);
  });
};


Insert.prototype._add_udp = function(packet, callback) {
  var self = this;

  if (!packet.protocol_name == "UDP" || !packet.ip.udp) {
    return callback(); 
  }

  var query = "INSERT INTO " +
    "udphdr (sid, cid, udp_sport, udp_dport, udp_len, udp_csum) " +
    "VALUES (?, ?, ?, ?, ?, ?)";

  var q = self.db(query);
  var event = self.event.event;

  var params = [self.sid, self.cid, packet.ip.udp.sport, packet.ip.udp.dport, 
    packet.ip.udp.length, packet.ip.udp.checksum];

  q.execute(params, function(err, result) {
    
    if (err) {
      return callback(err);
    };

  });
};



Insert.prototype._add_ip_hdr = function(event, callback) {
  var self = this;

  if (!self.event.packets || !self.event.packets.length > 0) {
    debug('no ip hdr');
    return callback("Missing ip packet");
  }
  var packet = self.event.packets[0].packet;

  //debug('packet:', packet, self.event);
  if (!packet.ip) {
    debug("Missing ip hdr in packet", packet); 
    return callback("Missing ip hdr in packet");
  };

  var query = 'insert into iphdr  (sid, cid, ip_src, ip_dst, ip_hlen,' +
    "ip_tos, ip_len, ip_id, ip_flags, ip_off," +
    "ip_ttl, ip_proto, ip_csum) " +
    'VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)';
  
  var q = self.db(query);
  
  var params = [self.sid, self.cid, event.source_ip, event.destination_ip, 
    packet.ip.header_length,
    0, packet.ip.total_length, packet.ip.identification, 
    0, // TODO: packet.ip.flags is 3 bits. make int?
    packet.ip.fragment_offset,   
    packet.ip.ttl,
    packet.ip.protocol,
    packet.ip.header_checksum
  ];

  q.execute(params, function(err, result) {
    
    if (err) {
      callback(err)
    };

    if (packet.ip.udp) {
      self._add_udp(packet, callback); 
    } else if (packet.ip.tcp) {
      self._add_tcp(packet, callback); 
    } else if (packet.ip.icmp) {
      self._add_icmp(packet, callback); 
    } else {
      callback(null);
    }
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
        // debug('adding event: ', sensor, self.event);
        if (err) {
          return self.emit('error', err);
        }

        self._add_ip_hdr(event.event, function(err) {
          // debug('adding event: ', sensor, self.event);
          if (err) {
            return self.emit('error', err);
          }

          self._add_payload(function(err) {

            if (err) {
              return self.emit('error', err);
            };

            self.emit('end');
          })
        });

      })
   
    })
  });
};

module.exports = Insert;



