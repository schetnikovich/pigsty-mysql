var mysql = require('mysql-native');
var debug = require('debug')('pigsty-mysql');
var EventEmitter = require("events").EventEmitter;
var util = require('util');

function ConnectionPool(options) {
  var self = this;
  options = options || {};
  self.max_size = options.max_pool_size || 2;
  self.options = options;
  self.free = {};
  self.used = {};
  self.id = 0;
  self.pending = [];

  self.watch = setInterval(function() {
  //  debug('looking for free', Object.keys(self.used), self.pending.length);
    self.assign_free();
  }, 1000);

};


ConnectionPool.prototype.assign_free = function() {
  var self = this;
  while (self.has_free() && self.pending.length > 0) {
    var callback = self.pending.pop(); 
    self.get(callback);
  }
};

ConnectionPool.prototype.stop = function() {
  // XXX: todo, stop
};

ConnectionPool.prototype.release = function(connection) {
  var self = this;
  var id = connection._pool_id;
  var connection = self.used[id];

  if (connection) {
    debug('releasing connection:', id);
    self.free[id] = connection;
    delete self.used[id];
  } else {
    console.error("connection already freed:", id);
  }

  self.assign_free();
 
};

ConnectionPool.prototype.total = function() {
  var self = this;
  return Object.keys(self.free).length + Object.keys(self.used).length;
};

ConnectionPool.prototype.has_free = function() {
  var self = this;
  return (self.total() < self.max_size || Object.keys(self.free).length > 0);
};


ConnectionPool.prototype.get = function(callback) {
  var self = this;
  var free_keys = Object.keys(self.free);
  var attempts = 0;

  var test = function(connection) {
    attempts++;

    if (connection.bad) {
      delete self.used[id];
      return self.get(callback);
    };

    connection.test(function(err, ok) {
      if (err) {
        console.error("[X] Unable to fetch connection from pool... retrying in 5 seconds:", attempts)
        delete self.used[id];
        if (attempts < 10) return setTimeout(function() { self.get(callback); }, 5000);
        else {
          return callback(err); 
        } 
      } else {
        debug('using available connection', assign);
        return callback(null, connection);
      }
    });
  };  
 
  if (free_keys.length > 0) {
    var assign = free_keys.pop();
    var connection = self.free[assign]; 
    self.used[assign] = connection; 
    delete self.free[assign];
    return test(connection);
  }

  if (self.total() >= self.max_size) {
//    debug('connections all in use; enqueueing');
    self.pending.push(callback);
  } else { // create a new one
    var id = "pool_id_" + self.id; 
    self._connection(id, function(err,connection) {
      self.id += 1; 
      self.used[id] = connection;
      debug('allocating new connection',id); 
      callback(null, self.used[id]);
    });
  }    
}

ConnectionPool.prototype._connection = function(id, callback) {
  var self = this;
  var connection =  mysql.createTCPClient(self.options.host, self.options.port || 3306);
  connection.auto_prepare = true;

  connection.test = function(callback) {
    var q = connection.execute("select 1");
    q.on('error', function(err) {
      console.error("warning: connection test failed:", connection._pool_id, err);
      connection.bad = true;
      return callback(err); 
    });
    q.on('end', function(err) {
      return callback(null, connection);
    });
  };
  connection.auth(self.options.database || "snorby", 
                  self.options.user, 
                 self.options.password);
  connection._pool_id = id; 
  connection.on('authorized', function(data) {
    console.log("auth data", data); 
//    callback(null, connection);
  });

  connection.exec = function(query, parameters) {
    connection._active_query = connection.execute(query, parameters);
    // console.log("XXX EXECUTING", connection._pool_id, query, parameters, connection._active_query);
    if (connection.bad) {
      // console.log("XXX CONNECTION BAD IS", connection.bad, connection._pool_id)
      return null;
    } else {
      return connection._active_query;
    }
  };

  connection.on('error', function(data) {
    console.error("warning: connection failed, setting connection to bad.", connection._pool_id, data, connection.bad, connection._active_query);
    connection.bad = true;
    if (connection._active_query) {
      connection._active_query.emit('error', data);
      connection._active_query.emit('end');
    } 
  
  })
  return callback(null,connection);
};


Query.prototype.constructor = Query;

function Query(connection, query) {
  EventEmitter.call(this);
  var self = this;
  self.query = query; 
  self.container = connection;
};

util.inherits(Query, EventEmitter);

Query.prototype._connection = function(callback) {
  var self = this;
  if (self.container.connection.bad) {
    // try to get a new connection.
    debug('connection is bad; getting new connection');
    self.container.pool.get(function(err, connection) {
      if (err) return callback(err);
      self.container.connection = connection;
      return callback(null, connection);
    });
  } else {
    return callback(null, self.container.connection);
  }
};


Query.prototype.close = function() {
  var self = this;
  // no-op?
};

Query.prototype.retryable = function(work, callback) {

  var self = this;
  var attempts = 0;
  
  var run = function() {

    work(function(err, result) {
      attempts++;
      if (err) {
        if (attempts > 5) {
          console.error("ERROR: Failure executing:" , self.query, "too many attempts:", attempts);
          return callback(err);
        } else {
          debug("Unable to execute:" , self.query, "retrying:", attempts);
          setTimeout(function() { run(); }, 5000);
        }
      } else {
        if (attempts > 1) debug('connection ok: re-established after retrying:', self.query, attempts)
        return callback(null, result);
      }

    });
  };

  run();
};

Query.prototype.selectOne = function(parameters, callback) {
  var self = this;

  var work = function(cb) {

    self._connection(function(err, connection) {
      var exec = connection.exec(self.query,parameters);

      if (!exec) return cb("bad connection: " + self.query);

      var rows = [];
      self.error = null;

      exec.on('error', function(result) {
        connection.bad = true;
        debug('got error: ', self.query, parameters, result)
        self.error = result;
      });

      exec.on('row', function(data) {
        // debug('received:', data);
        rows.push(data);
      });

      exec.on('end', function() {
        connection._active_query = null;
        var result = null;
        debug('selected: ', self.query, "on: ", connection._pool_id);
        if (rows.length > 0) {
          result = rows[0];
        }
        return cb(self.error, result);
      })
    });
  };

  self.retryable(work, callback);
};


Query.prototype.execute = function(parameters, callback, retryable) {
  var self = this;

  var work = function(cb) {
    self._connection(function(err, connection) {
      var exec = connection.exec(self.query,parameters);
      if (!exec) return cb("bad connection:" + self.query);
      self.result = null;
      self.error = null;

      exec.on('error', function(result) {
        debug('got error: ', self.query, parameters, result);
        self.error = result;
      });

      exec.on('result', function(result) {
        //      debug('got result: ', result)
        self.result = result;
      });

      exec.on('end', function() {
        connection._active_query = null;
        debug('executed: ', self.query, "on: ", connection._pool_id);
        return cb(self.error, self.result);
      });

    });

  };

  if (retryable) {
    self.retryable(work, callback);
  } else {
    work(callback);
  };
  
};



Query.prototype.select = function(parameters, callback) {
  var self = this;

  var work = function(cb) {
    self._connection(function(err, connection) {
      var exec = connection.exec(self.query,parameters);
      if (!exec) return cb("bad connection:" + self.query);
      var rows = [];
      self.error = null;

      exec.on('error', function(result) {
        debug('got error: ', self.query, parameters, result)
        self.error = result;
      });

      exec.on('row', function(data) {
        rows.push(data);
      });

      exec.on('end', function() {
        connection._active_query = null;
        debug('executed: ', self.query, "on: ", connection._pool_id);
        return cb(self.error, rows);
      })
    });
  };

  self.retryable(work, callback);
};



var fn = function(options, callback) {
  var self = this;
  
  if (!module.pool) {
    module.pool = new ConnectionPool(options);
  } 

  var pool = module.pool; 

  var container = {
     connection: null,
     pool: module.pool
  };

  pool.get(function(err, connection) {

    container.connection = connection;
    container.pool = module.pool;

    if (err) {
      return callback(err);
    }

    var result = function(query) {
      return new Query(container, query);
    };

    result.stop = function() {
      pool.release(container.connection);
    }
    
    return callback(null, result); 

  });

};

module.exports = fn;

