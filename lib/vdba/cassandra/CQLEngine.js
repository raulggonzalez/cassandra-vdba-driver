//imports
var CassandraResult = require("./Result").CassandraResult;

//api
exports.CQLEngine = CQLEngine;

/**
 * The CQL engine.
 *
 * @class vdba.cassandra.CQLEngine
 * @private
 */
function CQLEngine(cx) {
  /**
   * The connection to use.
   *
   * @name connection
   * @type {vdba.cassandra.CassandraConnection}
   * @memberof vdba.cassandra.CQLEngine#
   */
  Object.defineProperty(this, "connection", {value: cx});
}

/**
 * Executes a CQL command.
 *
 * @name run
 * @function
 * @memberof vdba.cassandra.CQLEngine#
 *
 * @param {String} cql          The CQL command.
 * @param {Function} [callback] The function to call: fn(error).
 */
CQLEngine.prototype.run = function run(cql, callback) {
  this.connection.native.execute(cql, function(error) {
    if (callback) {
      if (error) callback(error);
      else callback();
    }
  });
};

/**
 * Executes a CQL command that returns a result.
 *
 * @name find
 * @function
 * @memberof vdba.cassandra.CQLEngine#
 *
 * @param {String} cql        The CQL command.
 * @param {Function} callback The function to call: fn(error, result).
 */
CQLEngine.prototype.find = function find(cql, callback) {
  this.connection.native.execute(cql, function(error, result) {
    if (error) callback(error);
    else callback(undefined, new CassandraResult(result.rows));
  });
};

/**
 * Executes a CQL command and returns the first row.
 *
 * @name findOne
 * @function
 * @memberof vdba.cassandra.CQLEngine#
 *
 * @param {String} cql        The CQL command.
 * @param {Function} callback The function to call: fn(error, row).
 */
CQLEngine.prototype.findOne = function findOne(cql, callback) {
  var one = false;

  function process(ix, row) {
    if (!one) {
      one = true;
      callback(undefined, row);
    }
  }

  this.connection.native.eachRow(cql, process, function(error) {
    if (error && !one) {
      callback(error);
    }
  });
};