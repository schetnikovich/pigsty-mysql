var debug = require('debug')('pigsty')

function SigClasses(options) {
  var self = this;
  self.db = options.db;
  self.sig_classes = {};
  self.locked = false; // ghettolock 
};

SigClasses.prototype.lookup = function(classification, callback) {
  var self = this;
  
  if (!classification) {
    return callback("No classification provided");
  };

  if (self.sig_classes[classification.name]) {
    return callback(null, self.sig_classes[classification.name]);
  };

  if (self.locked) {
    // wait a couple of secs
    return setTimeout(function() {
      debug('classifications lock busy...');
      self.lookup(classification, callback); 
    }, 1000) 
  }

  self.locked = true;

  self._fetch(classification, function(err, id) {
    self.locked = false;
    if (err) {
      return callback(err); 
    } else {
      self.sig_classes[classification.name] = id;
      return callback(null, self.sig_classes[classification.name]);
    } 
  })
};

SigClasses.prototype._fetch = function(classification,
                                       callback) {
  var self = this;

  var query = 'select sig_class_id from sig_class where \
  sig_class_name = ?';
  
  var params = [ classification.name ];

  var find =  self.db(query);

  find.selectOne(params, function(err, result) {

    if (err) {
      return callback(err); 
    }

    if (result) {
      return callback(null, result.sig_class_id) ;
    };

    // otherwise, insert
    var query = 'insert into sig_class (sig_class_name) values (?)';

    var insert = self.db(query);

    insert.execute(params, function(err, result) {
      callback(err, result.insert_id);
    }); 
  });
}

Signatures.prototype._lookup_sigclass = function(event, callback) {
  var self = this;
  self.sigclasses.lookup(event.classification, callback);
};

Signatures.prototype._fetch = function(event, callback) {
  var self = this;

  self._lookup_sigclass(event, function(err, sig_class_id) {


    var query = 'select sig_id from signature where \
    sig_sid = ? and sig_gid = ?';

    var params = [ event.signature_id, event.generator_id ];

    var find =  self.db(query);

    find.selectOne(params, function(err, sig_id) {

      if (err) {
        return callback(err); 
      }

      if (sig_id) {
        return callback(null, sig_id) ;
      };

      // otherwise, insert
      var query = 'insert into signature (sig_sid, sig_gid, \
      sig_class_id, sig_name, sig_priority, sig_rev) values (?,?,?,?,?,?)';

      var insert = self.db(query);
      var params = [ event.signature_id, event.generator_id,
        sig_class_id,
        event.signature.name,
        event.classification.severity,
        event.signature_revision
      ];

      insert.execute(params, function(err, result) {
        callback(err, result.insert_id);
      }); 
    }); 

  });
}

function Signatures(options) {
  var self = this;
  self.db = options.db;
  self.sigclasses = new SigClasses({
    db: self.db 
  });
  self.locked = false; // ghettolock 
  self.signatures = {};
};

Signatures.prototype.lookup = function(event, callback) {
  var self = this;

  if (!event) {
    return callback("No event provided");
  };

  if (!event.signature_id) {
    return callback("No signature_id provided");
  };

  event.generator_id = event.generator_id || 1;

  event.signature = event.signature || { 
    name: 'Snort Alert [' + event.signature_id + ':' + event.generator_id + ':0]'
  };

  var id = event.signature_id 
      + "_" + event.generator_id
      + "_" + event.signature_revision
      + "_" + event.signature.name;  

  if (self.signatures[id]) {
    return callback(null, self.signatures[id]);
  };
 
  if (self.locked) {
    // wait a couple of secs
    return setTimeout(function() {
      debug('signatures lock busy...');
      self.lookup(event, callback); 
    }, 1000) 
  }

  self.locked = true;

  self._fetch(event, function(err, sig) {
    debug("loading signature: ", sig);
    self.locked = false;
    if (err) {
      return callback(err); 
    } else {
      self.signatures[id] = sig;
      return callback(null, self.signatures[id]);
    } 
  });
};


module.exports = Signatures;


