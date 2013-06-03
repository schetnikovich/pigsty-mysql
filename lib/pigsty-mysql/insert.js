var debug = require('debug')('pigsty-mysql')
var PigstyPlugin = require('pigsty-plugin');
var EventEmitter = require('events').EventEmitter;
var util = require('util');
var moment = require('moment');

Insert.prototype.constructor = Insert;

util.inherits(Insert, EventEmitter);

var TCP = 6;
var UDP = 17;
var ICMP = 1;


function tcpflags(flags) {
  var val = 0;

  var keys = Object.keys(flags);
  var pos = keys.length - 1;
  for (var pos; pos >= 0; pos--) {
      var k = keys[pos];
      var flag = flags[k];
      if (flag) {
            val = val | (1 << (keys.length - 1 - pos));
          }
    };
  return val;
};

/**
 * TODO: clean up this file and code. it looks like buttblood
 *
 */
function Insert(options) {

  EventEmitter.call(this);

  var self = this;
  self.db = options.db;
  self.encoding = options.encoding || 'hex';
  self.sensors = options.sensors;
  self.signatures = options.signatures;
  self.event = options.event;
  self.utc = true;

  // if set, this will insert events into the databaes's local time.
  if (options.localtime) {
    self.utc = false;
  }

};

Insert.prototype._add_tcp_options = function(packet, callback) {
  // TODO: this doesn't work at all. need to add options support.
  // once i can figure out a sane way to get them from node-pcap 
  var self = this;

  return callback();

//  console.log("TCP OPTIONS", packet.ip.tcp.options);

  if (packet && packet.ip && packet.ip.tcp && packet.ip.tcp.options) {

    var keys = Object.keys(packet.ip.tcp.options);
    var todo = keys.length;

    for (var i in keys) {

      var opt = keys[i];

      var query = "INSERT INTO " +
        "opt (sid,cid,optid,opt_proto,opt_code,opt_len,opt_data) " +
        "VALUES (?,?,?,?,?,?,?)";
      
      var q = self.db(query);

      var params = [self.sid, self.cid, data]; 

      q.execute(params, function(err, result) {

        if (err) {
          return callback(err);
        };

        callback(null);
      });
    }
  } else {
    callback();
  };
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


Insert.prototype._add_icmp_short = function(event, callback) {
  
  var self = this;

  var query = "INSERT INTO " +
  "icmphdr (sid, cid, icmp_type, icmp_code) " +
  "VALUES (?,?,?,?)";

  var q = self.db(query);
  var event = self.event.event;

  // source_port is icmp type
  // and dest_port is code in unified2 event
  // http://manual.snort.org/node44.html
  var params = [self.sid, self.cid, event.source_port, event.dest_port];

  q.execute(params, function(err, result) {
 
    if (err) {
      return callback(err);
    };
   
    callback(null);
  });
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



Insert.prototype._add_tcp_short = function(event, callback) {
  var self = this;

  var query = "insert into tcphdr (sid, cid, tcp_sport, tcp_dport, " +
  "        tcp_flags) " +
  "VALUES (?,?,?,?,?)";

  var q = self.db(query);
 
  // XXX: setting 0 for flags.  I don't know what belongs here.
  var params = [self.sid, self.cid, event.source_port, event.dest_port, 
    0
  ];

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
    packet.ip.tcp.seqno, packet.ip.tcp.ackno, packet.ip.tcp.header_bytes / 4,
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



Insert.prototype._add_udp_short = function(event, callback) {
  var self = this;

  var query = "INSERT INTO " +
    "udphdr (sid, cid, udp_sport, udp_dport) " +
    "VALUES (?, ?, ?, ?)";

  var q = self.db(query);

  var params = [self.sid, self.cid, event.source_port, event.dest_port];

  q.execute(params, function(err, result) {
    
    if (err) {
      return callback(err);
    };

    return callback(null);

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

    return callback(null);

  });
};


/**
 * If there's no packets, still populate the data.  
 */
Insert.prototype._add_ip_hdr_short = function(event, callback) {
  var self = this;
  //console.log("no ip hdr", this.sid, this.cid);

  var query = 'insert into iphdr  (sid, cid, ip_src, ip_dst, ip_proto)' +
    'VALUES (?,?,?,?,?)';
  
  var q = self.db(query);
  
  var params = [self.sid, self.cid, event.source_ip, event.destination_ip, 
    event.protocol
  ];

  var protocol = event.protocol;
 
  q.execute(params, function(err, result) {
    
    if (err) {
      callback(err)
    };

    if (protocol == UDP) {
     self._add_udp_short(event, callback); 
    } else if (protocol == TCP) {
      self._add_tcp_short(event, callback); 
    } else if (protocol == ICMP) {
      self._add_icmp_short(packet, callback); 
    } else {
      console.error("Unknown protocol: ", protocol, event);
      callback(null);
    }
  });



};

Insert.prototype._add_ip_hdr = function(event, callback) {
  var self = this;

  if (!self.event.packets || !self.event.packets.length > 0) {
    return self._add_ip_hdr_short(event, callback);
  };

  var packet = self.event.packets[0].packet;

  //debug('packet:', packet, self.event);
  if (!packet.ip) {
    debug("Missing ip hdr in packet", packet); 
    return callback("Missing ip hdr in packet");
  };


  var query = 'insert into iphdr  (sid, cid, ip_src, ip_dst, ip_hlen,' +
    "ip_tos, ip_len, ip_id, ip_flags, ip_off," +
    "ip_ttl, ip_proto, ip_csum, ip_ver) " +
    'VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)';
  
  var q = self.db(query);
  
  var params = [self.sid, self.cid, event.source_ip, event.destination_ip, 
    packet.ip.header_length,
    0, packet.ip.total_length, packet.ip.identification, 
    0, // TODO: packet.ip.flags is 3 bits. make int?
    packet.ip.fragment_offset,   
    packet.ip.ttl,
    packet.ip.protocol,
    packet.ip.header_checksum,
    packet.ip.version
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
  var query = 'insert into event (sid, cid, signature, timestamp)'
  var args;

  if (self.utc) {
    query += ' values (?, ?, ?, ?)';
    var time = moment.unix(event.event_second).utc();
    args = [self.sid, self.cid, self.sig_id, time.format("YYYY-MM-DD HH:mm:ss")];
  } else {
    query += ' values (?, ?, ?, FROM_UNIXTIME(?))';
    args = [self.sid, self.cid, self.sig_id, event.event_second];
  }

  var q = self.db(query);

  q.execute(args, function(err, result) {
    
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


Insert.prototype._error_and_end = function(msg) {
  var self = this;
  self.emit('error', msg);
  self.emit('end');

};
Insert.prototype.run = function() {
  var self = this;
  var event = self.event;

//  return self.emit('end'); // XXX

  if (!event || Object.keys(self.event) == 0) {
    return; 
  };

  if (!event.sensor) {
    return self._error_and_end({ msg: "No sensor for event: " , event: event });
  };

  if (!event.event_type) {
    debug('missing event type: ', event);
    return self._error_and_end("No event type for event: " + event);
  };

  self.sensors.lookup(event.sensor, function(err, sensor) {

    if (err) {
      return self._error_and_end(err);
    }

    self.sid = sensor.sid;
    self.cid = sensor.increment_cid();

    self._signature(event.event, function(err) {

      if (err) {
        return self._error_and_end(err);
      }

      self._add_event(event.event, function(err) {
        // debug('adding event: ', sensor, self.event);
        if (err) {
          return self._error_and_end(err);
        }

        self._add_ip_hdr(event.event, function(err) {
          
          if (err) {
            return self._error_and_end(err);
          }

          self._add_payload(function(err) {

            if (err) {
              return self._error_and_end(err);
            };

            self.emit('end');
          })
        });

      })
   
    })
  });
};

module.exports = Insert;



