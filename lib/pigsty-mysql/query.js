// var mysql = require('mysql-native');
var mysql = require('mysql2');
var debug = require('debug')('pigsty-mysql');
var EventEmitter = require("events").EventEmitter;
var util = require('util');

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


Query.prototype.constructor = Query;

function Query(connection, query) {
  EventEmitter.call(this);
  var self = this;
  self.query = query; 
  self.connection = connection;
};

util.inherits(Query, EventEmitter);

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
    debug('starting: selectOne: ', self.query, parameters);
    connection.execute(self.query,parameters, function(err, rows) {
      if (err) return callback(err);
      debug('selectONe: ', self.query, "returned", rows);
   
      var result = null;
      if (rows.length > 0) {
        result = rows[0];
      }
      return callback(null, result);
    });
  });
};


Query.prototype.execute = function(parameters, callback) {
  var self = this;

  self._connection(function(err, connection) {
    debug('starting execute: ', self.query, parameters, "on: ");
    connection.execute(self.query,parameters, function(err, rows) {
      debug('executed: ', self.query, "returned:", rows);
      if (err) return callback(err);

      if (rows.insertId)
        rows.insert_id = rows.insertId;

      return callback(null, rows);
    });
  });
};



Query.prototype.select = function(parameters, callback) {
  var self = this;
  self._connection(function(err, connection) {
    debug('starting select: ', self.query, parameters);
    connection.execute(self.query,parameters, function(err, rows) {
      if (err) return callback(err);
      debug('selected: ', self.query, "returned", rows);
      return callback(null, rows);
    });
  });
};


var fn = function(options, callback) {
  var self = this;
  
  if (!module.pool) {
    var pool_options = {
      database: options.database || "snorby", 
      user: options.user, 
      password: options.password 
    };

    if (options.max_pool_size) {
      pool_options.connectionLimit = options.max_pool_size + 1;
    }

    module.pool = mysql.createPool(pool_options);
  }

  var pool = module.pool; 

  pool.getConnection(function(err, connection) {

    if (err) {
      return callback(err);
    }

    var result = function(query) {
      return new Query(connection, query);
    };

    result.stop = function() {
      pool.releaseConnection(connection);
    };
    
    return callback(null, result); 
  });

};

module.exports = fn;

