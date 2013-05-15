var debug = require('debug')('pigsty-mysql')

function RefSystems(options) {
  var self = this;
  self.db = options.db;
  self.ref_systems = {};
  self.locked = false; // ghettolock 
};

RefSystems.prototype.lookup = function(reference, callback) {
  var self = this;

  if (!reference) {
    return callback("No reference provided");
  };

  if (self.ref_systems[reference.key]) {
    return callback(null, self.ref_systems[reference.key]);
  };

  if (self.locked) {
    // wait a couple of secs
    return setTimeout(function() {
      debug('references lock busy...');
      self.lookup(reference, callback); 
    }, 1000) 
  }

  self.locked = true;

  self._fetch(reference, function(err, id) {
    self.locked = false;
    if (err) {
      return callback(err); 
    } else {
      self.ref_systems[reference.key] = id;
      return callback(null, self.ref_systems[reference.key]);
    } 
  })
};

RefSystems.prototype._fetch = function(reference, callback) {
  var self = this;

  var query = 'select ref_system_id from reference_system where \
  ref_system_name = ?';

  var params = [ reference.key ];

  var find =  self.db(query);

  find.selectOne(params, function(err, result) {

    if (err) {
      return callback(err); 
    }

    if (result) {
      return callback(null, result.ref_system_id) ;
    };

    // otherwise, insert
    var query = 'insert into reference_system (ref_system_name) values (?)';

    var insert = self.db(query);

    insert.execute(params, function(err, result) {

      if (err) {
        return callback(err);
      }

      callback(null, result.insert_id);
    }); 
  });
}

References.prototype._lookup_ref_system_id = function(reference, callback) {
  var self = this;
  self.ref_systems.lookup(event.classification, callback);
};


function References(options) {
  var self = this;
  self.db = options.db;
  self.ref_systems = new RefSystems({
    db: self.db 
  });
};

References.prototype.lookup = function(reference, callback) {
  var self = this;

  if (!reference) {
    return callback("No reference provided");
  };

  self.ref_systems.lookup(reference, function(err, ref_system_id) {

    if (err) {
      return callback(err);
    }

    var query = 'select ref_id from reference where \
    ref_system_id = ? and ref_tag = ?';

    var params = [ ref_system_id, reference.value ];
    var find =  self.db(query);

    find.selectOne(params, function(err, result) {

      if (err) {
        return callback(err); 
      }

      if (result) {
        return callback(null, result.ref_id) ;
      };

      if (self.locked) {

        // wait a couple of secs
        return setTimeout(function() {
          debug('references lock busy...');
          self.lookup(reference, callback); 
        }, 1000);
      };

      self.locked = true;
      // otherwise, insert
      var query = 'insert into reference (ref_system_id, ref_tag) \
      values (?,?)';

      var insert = self.db(query);

      insert.execute(params, function(err, result) {
        self.locked = false;
        if (err) {
          return callback(err);
        }
        callback(err, result.insert_id);
      }); 

    })
  })
}

module.exports = References;


