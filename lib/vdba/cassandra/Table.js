//imports
var util = require("util");
var vdba = require("../../index");

//api
exports.CassandraTable = CassandraTable;

/**
 * A C* table.
 *
 * @class vdbn.cassandra.CassandraTable
 * @extends vdba.Table
 * @protected
 *
 * @param {vdba.cassandra.CassandraDatabase} db The database object.
 * @param {String} name                         The table name.
 */
function CassandraTable(db, name) {
  CassandraTable.super_.call(this, db, name);
}

util.inherits(CassandraTable, vdba.Table);