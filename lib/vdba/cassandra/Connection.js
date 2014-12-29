//imports
var util = require("util");
var cassandra = require("cassandra-driver");
var vdba = require("../../index");
var CassandraServer = require("./Server").CassandraServer;
var CassandraDatabase = require("./Database").CassandraDatabase;

//api
exports.CassandraConnection = CassandraConnection;

/**
 * A C* connection.
 *
 * @class vdba.cassandra.CassandraConnection
 * @extends vdba.Connection
 * @protected
 *
 * @param {Object} config The connection configuration object.
 * @param {Client} client The cassandra.Client object.
 */
function CassandraConnection(config, client) {
  CassandraConnection.super_.call(this, config);

  /**
   * The cassandra.Client object.
   *
   * @name native
   * @memberof vdba.cassandra.CassandraConnection#
   * @private
   */
  Object.defineProperty(this, "native", {value: client});
}

util.inherits(CassandraConnection, vdba.Connection);

/**
 * The read consistency.
 *
 * @name readConsistency
 * @type {String}
 * @memberof vdba.cassandra.CassandraConnection#
 */
CassandraConnection.prototype.__defineGetter__("readConsistency", function() {
  return this.config.readConsistency;
});

/**
 * The write consistency,
 *
 * @name writeConsistency
 * @type {String}
 * @memberof vdba.cassandra.CassandraConnection#
 */
CassandraConnection.prototype.__defineGetter__("writeConsistency", function() {
  return this.config.writeConsistency;
});

/**
 * Is it connected?
 *
 * @name connected
 * @type {Boolean}
 * @memberof vdba.cassandra.CassandraConnection#
 */
CassandraConnection.prototype.__defineGetter__("connected", function() {
  var res;

  //(1) check whether connected
  res = false;

  if (this.native.connected) {
    res = false;

    for (var i = 0, hosts = Object.keys(this.native.hosts.items); i < hosts.length; ++i) {
      if (!this.native.hosts.items[hosts[i]].closing) res = true;
    }
  }

  //(2) return
  return res;
});

/**
 * The C* server connected to.
 *
 * @name server
 * @type {vdba.cassandra.CassandraServer}
 * @memberof vdba.cassandra.CassandraConnection#
 */
CassandraConnection.prototype.__defineGetter__("server", function() {
  //(1) get server object
  if (this.connected && !this.cassandraServer) {
    Object.defineProperty(this, "cassandraServer", {value: new CassandraServer(this), writable: true});
  }

  //(2) return
  return this.cassandraServer;
});

/**
 * The current database.
 *
 * @name database
 * @type {vdba.cassandra.CassandraDatabase}
 * @memberof vdba.cassandra.CassandraConnection#
 */
CassandraConnection.prototype.__defineGetter__("database", function() {
  //(1) create object if needed
  if (this.connected && !this.currentDatabase) {
    Object.defineProperty(this, "currentDatabase", {value: new CassandraDatabase(this, this.native.keyspace)});
  }

  //(2) return
  return this.currentDatabase;
});

/**
 * Returns a new connection instance.
 *
 * @name getConnection
 * @function
 * @memberof vdba.cassandra.CassandraConnection
 * @protected
 *
 * @param {Object} config The connection configuration object.
 * @returns {vdba.cassandra.CassandraConnection}
 */
CassandraConnection.getConnection = function getConnection(config) {
  var defaultConsistency = "quorum";

  //(1) pre: check config object
  if (!config) {
    throw new Error("Configuration expected.");
  }

  //create new config to final config
  config = util._extend({}, config);

  //contact points
  if (!config.hosts) {
    config.hosts = ["localhost"];
  } else {
    config.hosts = (typeof(config.hosts) == "string" ? [config.hosts] : config.hosts);
  }

  //keyspace
  if (!config.database) {
    throw new Error("Database expected.");
  }

  //consistencies
  if (!config.readConsistency) config.readConsistency = defaultConsistency;
  if (!config.writeConsistency) config.writeConsistency = defaultConsistency;

  config.readConsistencyLevel = getConsistencyLevel(config.readConsistency);
  config.writeConsistencyLevel = getConsistencyLevel(config.writeConsistency);

  //(2) return new connection
  return new CassandraConnection(
    config,
    new cassandra.Client({
      keyspace: config.database,
      contactPoints: config.hosts,
      authProvider: new cassandra.auth.PlainTextAuthProvider(config.username, config.password)
    })
  );
};

function getConsistencyLevel(consistency) {
  var con;

  //(1) get consistency
  switch (consistency.toLowerCase()) {
    case "all": con = cassandra.types.consistencies.all; break;
    case "any": con = cassandra.types.consistencies.any; break;
    case "eachQuorum": con  = cassandra.types.consistencies.eachQuorum; break;
    case "localOne": con = cassandra.types.consistencies.localOne; break;
    case "localQuorum": con = cassandra.types.consistencies.localQuorum; break;
    case "one": con = cassandra.types.consistencies.one; break;
    case "quorum": con = cassandra.types.consistencies.quorum; break;
    case "three": con = cassandra.types.consistencies.three; break;
    case "two": con = cassandra.types.consistencies.two; break;
    default: throw new Error("Unknown consistency: " + consistency + ".");
  }

  //(2) return consistency
  return con;
}

/**
 * Opens the connection.
 *
 * @name open
 * @function
 * @memberof vdba.cassandra.CassandraConnection#
 *
 * @param {Function} [callback] The function to call: fn(error).
 */
CassandraConnection.prototype.open = function open(callback) {
  if (this.connected) {
    callback();
  } else {
    this.native.connect(function(error) {
      if (callback) {
        callback(error || undefined);
      }
    });
  }
};

/**
 * Closes the connection.
 *
 * @name close
 * @function
 * @memberof vdba.cassandra.CassandraConnection#
 *
 * @param {Function} [callback] The function to call: fn(error).
 */
CassandraConnection.prototype.close = function close(callback) {
  if (this.connected) {
    this.cassandraServer = undefined;
    this.native.shutdown(callback);
  } else {
    if (callback) callback();
  }
};

/**
 * Runs a function into a transaction.
 *
 * @name runTransaction
 * @function
 * @memberof vdba.Connection#
 *
 * @param {String} mode         The transaction mode: readonly or readwrite.
 * @param {Function} op         The operation to run into a transaction.
 * @param {Function} [callback] The function to call: fn(error).
 *
 * @example
 * cx.runTransaction("readonly", function(db) { ... });
 * cx.runTransaction("readonly", function(db) { ... }, function(error) { ... });
 */
CassandraConnection.prototype.runTransaction = function runTransaction() {
  throw new Error("Abstract method.");
};