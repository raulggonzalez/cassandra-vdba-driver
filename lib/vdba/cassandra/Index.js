//imports
var util = require("util");
var vdba = require("../../index");

//api
exports.CassandraIndex = CassandraIndex;

/**
 * A C* index.
 *
 * @class vdba.cassandra.CassandraIndex
 * @extends vdba.Index
 * @protected
 *
 * @param {vdba.cassandra.CassandraTable} table The table.
 * @param {String} name                         The index name.
 */
function CassandraIndex(table, name) {
  CassandraIndex.super_.call(this, table, name);
}

util.inherits(CassandraIndex, vdba.Index);