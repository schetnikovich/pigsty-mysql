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
};

ConnectionPool.prototype.destroy_statements = function(query) {
  var self = this;
  for (var i in self.free) {
    self.free[i].remove_statements(query);
  };
  // XXX: TODO: used ones?
};


ConnectionPool.prototype.has_free = function() {
  var self = this;
  return (self.total <= self.max_size || Object.keys(self.free).length > 0);
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
    var watch = setTimeout(function() {
      debug("waiting for free connection...");
      if (self.has_free()) {
       return self.get(callback);
      } else {
        watch();
      }
      
    }, 1000);
  
  } else { // create a new one
    var id = self.total; 
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


function Query(pool, query) {
  var self = this;
  self.query = query; 
  self.pool = pool;
};

Query.prototype = new EventEmitter(); 

Query.prototype._connection = function(callback) {
  var self = this;
  self.pool.get(function(err, connection) {
    callback(err, connection);
  });
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
      debug('received:', data);
      rows.push(data);
    });
    
    exec.on('end', function() {
      self.pool.release(connection);
      var result = null;
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
      debug('got result: ', result)
      self.result = result;
    });

    exec.on('end', function() {
      debug('ended');
      self.pool.release(connection);
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
      debug('received:', data);
      rows.push(data);
    });

    exec.on('end', function() {
      self.pool.release(connection);
      return callback(null, rows);
    })
  });
};


var fn = function(options) {
  var self = this;
  
  if (!module.pool) {
    module.pool = new ConnectionPool(options);
  } 
  var pool = module.pool; 

  var result = function(query) {
    return new Query(pool, query);
  };

  result.pool = function() {
    return module.pool;
  };

  result.stop = function() {
    if (module.pool) {
      module.pool.stop();
      module.pool = null;
    };
  }

  return result;
};

module.exports = fn;

