var mysql = require('mysql-native');
var debug = require('debug')('pigsty-mysql');
var EventEmitter = require("events").EventEmitter;

function ConnectionPool(options) {
  var self = this;
  options = options || {};
  self.max_size = options.max_pool_size || 2;
  self.options = options;
  self.total = 0;
  self.free = {};
  self.used = {};
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

ConnectionPool.prototype.has_free = function() {
  var self = this;
  return (self.total < self.max_size || Object.keys(self.free).length > 0);
};

ConnectionPool.prototype.get = function(callback) {
  var self = this;
  var free_keys = Object.keys(self.free);
 
  if (free_keys.length > 0) {
    var assign = free_keys.pop();
    var connection = self.free[assign]; 
    debug('using available connection', assign);
    self.used[assign] = connection; 
    delete self.free[assign];
    return callback(null, connection);
  }

  if (self.total >= self.max_size) {
//    debug('connections all in use; enqueueing');
    self.pending.push(callback);
  } else { // create a new one
    var id = "pool_id_" + self.total; 
    self._connection(id, function(err,connection) {
      self.total += 1; 
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
  connection.auth(self.options.database || "snorby", 
                  self.options.user, 
                 self.options.password);
  connection._pool_id = id; 
  connection.on('authorized', function(data) {
    console.log("auth data", data); 
//    callback(null, connection);
  });

  connection.on('error', function(data) {
    console.error("connection error: ", data);
  })
  return callback(null,connection);
};


function Query(connection, query) {
  var self = this;
  self.query = query; 
  self.connection = connection;
};

Query.prototype = new EventEmitter(); 

Query.prototype._connection = function(callback) {
  var self = this;
  return callback(null, self.connection);
};

Query.prototype.close = function() {
  var self = this;
  // no-op?
};

Query.prototype.selectOne = function(parameters, callback) {
  var self = this;

  self._connection(function(err, connection) {
    var exec = connection.execute(self.query,parameters);
    var rows = [];
    self.error = null;

    exec.on('error', function(result) {
      debug('got error: ', self.query, parameters, result)
      self.error = result;
    });

    exec.on('row', function(data) {
      // debug('received:', data);
      rows.push(data);
    });
    
    exec.on('end', function() {
      var result = null;
      debug('executed: ', self.query, "on: ", connection._pool_id);
      if (rows.length > 0) {
        result = rows[0];
      }
      return callback(self.error, result);
    })
  });
};


Query.prototype.execute = function(parameters, callback) {
  var self = this;

  self._connection(function(err, connection) {

    var exec = connection.execute(self.query,parameters);
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
      debug('executed: ', self.query, "on: ", connection._pool_id);
      return callback(self.error, self.result);
    });

  });
};



Query.prototype.select = function(parameters, callback) {
  var self = this;
  self._connection(function(err, connection) {
    
    var exec = connection.execute(self.query,parameters);
    var rows = [];
    self.error = null;

    exec.on('error', function(result) {
      debug('got error: ', self.query, parameters, result)
      self.error = result;
    })
    exec.on('row', function(data) {
      rows.push(data);
    });

    exec.on('end', function() {
      debug('executed: ', self.query, "on: ", connection._pool_id);
      return callback(null, rows);
    })
  });
};


var fn = function(options, callback) {
  var self = this;
  
  if (!module.pool) {
    module.pool = new ConnectionPool(options);
  } 
  var pool = module.pool; 

  pool.get(function(err, connection) {

    if (err) {
      return callback(err);
    }

    var result = function(query) {
      return new Query(connection, query);
      
    };

    result.stop = function() {
      pool.release(connection);
    }
    
    return callback(null, result); 

  });

};

module.exports = fn;

