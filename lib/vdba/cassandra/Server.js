//imports
var util = require("util");
var vdba = require("../../index");

//API
exports.CassandraServer = CassandraServer;

/**
 * A C* server.
 *
 * @class vdba.cassandra.CassandraServer
 * @extends vdba.Server
 * @protected
 *
 * @param {vdba.cassandra.CassandraConnection} cx The connection to use.
 */
function CassandraServer(cx) {
  CassandraServer.super_.call(this);

  /**
   * The connection to use.
   *
   * @name connection
   * @type {vdba.cassandra.CassandraConnection}
   * @memberof vdba.cassandra.CassandraServer#
   * @private
   */
  Object.defineProperty(this, "connection", {value: cx});
}

util.inherits(CassandraServer, vdba.Server);

/**
 * The hostname.
 *
 * @name host
 * @type {String}
 * @memberof vdba.cassandra.CassandraServer#
 */
CassandraServer.prototype.__defineGetter__("host", function() {
  return (this.connection.native.options.contactPoints[0]);
});

/**
 * The port.
 *
 * @name port
 * @type {Number}
 * @memberof vdba.cassandra.CassandraServer#
 */
CassandraServer.prototype.__defineGetter__("port", function() {
  return this.connection.native.options.protocolOptions.port;
});

/**
 * The server version.
 *
 * @name version
 * @type {String}
 * @memberof vdba.cassandra.CassandraServer#
 */
CassandraServer.prototype.__defineGetter__("version", function() {
  throw new Error("Unsupported property.");
});