//imports
var util = require("util");
var cassandra = require("cassandra-driver");
var vdba = require("../../index");
var CassandraConnection = require("./Connection").CassandraConnection;

//api
exports.CassandraDriver = CassandraDriver;

/**
 * The C* VDBA driver.
 *
 * @class vdba.cassandra.CassandraDriver
 * @extends vdba.Driver
 * @protected
 */
function CassandraDriver() {
  CassandraDriver.super_.call(this, "Cassandra", ["C*"]);
}

util.inherits(CassandraDriver, vdba.Driver);

/**
 * Creates a C* connection.
 *
 * @name createConnection
 * @function
 * @memberof vdba.cassandra.CassandraDriver#
 *
 * @param {Object} config The configuration object: database (String), username (String),
 *                        password (String), hosts (String[]), readConsistency (String),
 *                        writeConsistency (String).
 *                        Consistencies: all, any, eachQuorum, localOne, localQuorum,
 *                        one, quorum, three, two.
 *
 * @returns {vdba.Connection}
 *
 * @example
 * cx = drv.createConnection({hosts: ["127.0.0.1:9042"], database: "myks", username: "user01", password: "pwd01"});
 */
CassandraDriver.prototype.createConnection = function createConnection(config) {
  return CassandraConnection.getConnection(config);
};