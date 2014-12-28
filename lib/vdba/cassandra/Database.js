//imports
var util = require("util");
var vdba = require("../../index");

//api
exports.CassandraDatabase = CassandraDatabase;

/**
 * A C* database/keyspace.
 *
 * @class odba.cassandra.CassandraDatabase
 * @extends odba.Database
 * @protected
 *
 * @param {vdba.cassandra.CassandraConnection} cx The connection to use.
 * @param {String} name                           The database name.
 */
function CassandraDatabase(cx, name) {
  CassandraDatabase.super_.call(this, name);

  /**
   * The connection to use.
   *
   * @name connection
   * @type {vdba.cassandra.CassandraConnection}
   * @memberof vdba.cassandra.CassandraDatabase#
   * @private
   */
  Object.defineProperty(this, "connection", {value: cx});
}

util.inherits(CassandraDatabase, vdba.Database);