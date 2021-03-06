//imports
var util = require("util");
var vdba = require("../../index");
var CQLEngine = require("./CQLEngine").CQLEngine;
var CassandraTable = require("./Table").CassandraTable;
var CassandraIndex = require("./Index").CassandraIndex;

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

  /**
   * The SQL engine to use.
   *
   * @name engine
   * @type {vdba.cassandra.CQLEngine}
   * @memberof vdba.cassandra.CassandraDatabase#
   * @private
   */
  Object.defineProperty(this, "engine", {value: new CQLEngine(this.connection)});
}

util.inherits(CassandraDatabase, vdba.Database);

/**
 * Creates a table.
 *
 * @name createTable
 * @function
 * @memberof vdba.cassandra.CassandraDatabase#
 *
 * @param {String} table
 * @param {Object} columns      The columns. Each column is a property where can have
 *                              the following value object: type (String), primaryKey or
 *                              pk (Boolean).
 * @param {Object} [options]    The create options: ifNotExists (Boolean), partitionKey or
 *                              pk (String or String[]), clusteringKey or ck (String or String[]).
 * @param {Function} [callback] The function to call: fn(error).
 */
CassandraDatabase.prototype.createTable = function createTable(table, columns, options, callback) {
  var cql;

  //(1) pre: arguments
  if (arguments.length == 3) {
    if (arguments[2] instanceof Function) {
      callback = arguments[2];
      options = undefined;
    }
  }

  if (!table) throw new Error("Table name expected.");
  if (!columns || Object.keys(columns).length < 1) throw new Error("Table columns expected.");
  if (!options) options = {};

  //(2) build CQL
  //header
  cql = "CREATE TABLE ";
  if (options.ifNotExists) cql += "IF NOT EXISTS ";
  cql += table;

  cql += "(";

  //columns
  for (var i = 0, names = Object.keys(columns); i < names.length; ++i) {
    var name = names[i], type;
    var col = columns[name];

    if (typeof(col) == "string") col = {type: col};

    cql += (i === 0 ? "" : ", ") + name + " " + col.type;

    if (col.pk || col.primaryKey) cql += " PRIMARY KEY";
  }

  //options
  if (options.pk || options.partitionKey) {
    var aux;

    cql += ", PRIMARY KEY(";

    aux = options.pk || options.partitionKey;
    if (!(aux instanceof Array)) aux = [aux];
    cql = "(" + aux.join(", ") + ")";

    if (options.ck || options.clusteringKey) {
      aux = options.ck || options.clusteringKey;
      if (!(aux instanceof Array)) aux = [aux];
      cql += ", " + aux.join(", ");
    }

    cql += ")";
  }

  cql += ")";

  //(3) run
  this.engine.run(cql, callback);
};

/**
 * Drops a table.
 *
 * @name dropTable
 * @function
 * @memberof vdba.cassandra.CassandraDatabase#
 *
 * @param {String} table        The table name.
 * @param {Function} [callback] The function to call: fn(error).
 */
CassandraDatabase.prototype.dropTable = function dropTable(table, callback) {
  //(1) pre: arguments
  if (!table) throw new Error("Table name expected.");

  //(2) run
  this.engine.run("DROP TABLE IF EXISTS " + table, callback);
};

/**
 * Checks whether a table exists.
 *
 * @name hasTable
 * @function
 * @memberof vbda.cassandra.CassandraDatabase#
 *
 * @param {String} table      The table name.
 * @param {Function} callback The function to call: fn(error, exists).
 */
CassandraDatabase.prototype.hasTable = function hasTable(table, callback) {
  var cql;

  //(1) pre: arguments
  if (!table) throw new Error("Table name expected.");
  if (!callback) throw new Error("Callback expected.");

  //(2) build cql
  cql = "SELECT count(*) as count " +
        "FROM system.schema_columnfamilies " +
        "WHERE keyspace_name=? AND columnfamily_name=?";

  //(3) check
  this.engine.findOnep(cql, [this.name, table.toLowerCase()], function(error, row) {
    if (error) callback(error);
    else callback(undefined, row.count == 1);
  });
};

/**
 * Checks whether several tables exist.
 *
 * @name hasTables
 * @function
 * @memberof vdba.cassandra.CassandraDatabase#
 *
 * @param {String[]} tables   The table names.
 * @param {Function} callback The function to call: fn(error, exist).
 */
CassandraDatabase.prototype.hasTables = function hasTables(tables, callback) {
  var cql;

  //(1) pre: arguments
  if (!tables || tables.length < 1) throw new Error("Table names expected.");
  if (!callback) throw new Error("Callback expected.");

  //(2) check
  cql = "SELECT columnfamily_name " +
        "FROM system.schema_columnfamilies " +
        "WHERE keyspace_name=? AND columnfamily_name in (";

  for (var i = 0; i < tables.length; ++i) {
    cql += (i === 0 ? "" : ", ") + "'" + tables[i].toLowerCase() + "'";
  }

  cql += ")";

  //(3) check
  this.engine.findp(cql, [this.name], function(error, result) {
    if (error) callback(error);
    else callback(undefined, result.length == tables.length);
  });
};

/**
 * Returns a table object.
 *
 * @name findTable
 * @function
 * @memberof vdba.cassandra.CassandraDatabase#
 *
 * @param {String} table      The table name.
 * @param {Function} callback The function to call: fn(error, table).
 */
CassandraDatabase.prototype.findTable = function findTable(table, callback) {
  var self = this;

  //(1) pre: arguments
  if (!table) throw new Error("Table name expected.");
  if (!callback) throw new Error("Callback expected.");

  //(2) find
  this.hasTable(table, function(error, exists) {
    if (error) {
      callback(error);
    } else {
      if (!exists) callback();
      else callback(undefined, new CassandraTable(self, table));
    }
  });
};

/**
 * Creates an index on a table.
 *
 * @name createIndex
 * @function
 * @memberof vdba.cassandra.CassandraDatabase#
 *
 * @param {String|vdba.Table} table The table.
 * @param {String} index            The index name.
 * @param {String|String[]} column  The column name.
 * @param {Object} [options]        The index options.
 * @param {Function} [callback]     The function to call: fn(error).
 */
CassandraDatabase.prototype.createIndex = function createIndex(table, index, column, options, callback) {
  var cql;

  //(1) pre: arguments
  if (arguments.length == 4) {
    if (arguments[3] instanceof Function) {
      callback = arguments[3];
      options = undefined;
    }
  }

  if (table instanceof vdba.Table) table = table.name;
  if (!table) throw new Error("Table expected.");
  if (!index) throw new Error("Index name expected.");
  if (!column) throw new Error("Indexing column expected.");

  //(2) build cql
  cql = "CREATE INDEX " + index + " ON " + table + "(" + column + ")";

  //(3) create
  this.engine.run(cql, callback);
};

/**
 * Drops an index on a table.
 *
 * @name dropIndex
 * @function
 * @memberof vdba.cassandra.CassandraDatabase#
 *
 * @param {String|vdba.Index} index The index.
 * @param {Function} [callback]     The function to call: fn(error).
 */
CassandraDatabase.prototype.dropIndex = function dropIndex(index, callback) {
  //(1) pre: arguments
  if (index instanceof vdba.Index) index = index.name;
  if (!index) throw new Error("Index expected.");

  //(2) drop
  this.engine.run("DROP INDEX " + index, function(error) {
    if (error && !/Index '.+' could not be found in any of the tables of keyspace/.test(error.message)) {
      if (callback) callback(error);
    } else {
      if (callback) callback();
    }
  });
};

/**
 * Checks whether an index exists.
 *
 * @name hasIndex
 * @function
 * @memberof vdba.cassandra.CassandraDatabase#
 *
 * @param {String|vdba.Index} index The index.
 * @param {Function} callback       The function to call: fn(error, exists).
 */
CassandraDatabase.prototype.hasIndex = function hasIndex(index, callback) {
  var self = this, cql;

  //(1) pre: arguments
  if (index instanceof vdba.Index) index = index.name;
  if (!index) throw new Error("Index expected.");
  if (!callback) throw new Error("Callback expected.");

  //(2) build cql
  cql = "SELECT index_name, columnfamily_name, column_name FROM system.schema_columns WHERE keyspace_name=?";

  //(3) check
  this.engine.findp(cql, [this.name], function(error, result) {
    if (error) callback(error);
    else {
      var aux = result.rows.filter(function(row) {
        return row.index_name == index;
      });

      callback(undefined, aux.length > 0);
    }
  });
};

/**
 * Returns an index object.
 *
 * @name findIndex
 * @function
 * @memberof vdba.cassandra.CassandraDatabase#
 *
 * @param {String} index      The index name.
 * @param {Function} callback The function to call: fn(error, index).
 */
CassandraDatabase.prototype.findIndex = function findIndex(index, callback) {
  var self = this, cql;

  //(1) pre: arguments
  if (index instanceof vdba.Index) index = index.name;
  if (!index) throw new Error("Index name expected.");
  if (!callback) throw new Error("Callback expected.");

  //(2) build cql
  cql = "SELECT index_name, columnfamily_name, column_name FROM system.schema_columns WHERE keyspace_name=?";

  //(3) check
  this.engine.findp(cql, [this.name], function(error, result) {
    if (error) {
      callback(error);
    } else {
      var aux = result.rows.filter(function(row) {
        return row.index_name == index;
      });

      if (aux.length < 1) {
        callback();
      } else {
        self.findTable(aux[0].columnfamily_name, function(error, table) {
          if (error) callback(error);
          else callback(undefined, new CassandraIndex(table, index));
        });
      }
    }
  });
};