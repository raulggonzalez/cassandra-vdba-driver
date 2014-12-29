//imports
var util = require("util");
var vdba = require("../../index");

//api
exports.CassandraResult = CassandraResult;

/**
 * A C* result.
 *
 * @class vdba.cassandra.CassandraResult
 * @extends vdba.Result
 * @protected
 *
 * @param {Object[]} rows The rows.
 */
function CassandraResult(rows) {
  CassandraResult.super_.call(this, rows);
}

util.inherits(CassandraResult, vdba.Result);