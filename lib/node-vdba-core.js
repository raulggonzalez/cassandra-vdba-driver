/*! vdba-core - 0.6.1 (2014-12-26) */

(function() {

/**
 * A combinator.
 *
 * @class vdba.Combinator
 * @private
 */
function Combinator() {

}

/**
 * Joins two row sets.
 *
 * @memberof vdba.Combinator#
 *
 * @param {Object[]} source   The source-side rows.
 * @param {Object[]} target   The target-side rows.
 * @param {String} sourceCol  The source-side column name.
 * @param {String} targetCol  The target-side column name.
 * @param {Object} opts       The join opts: arrayAgg (String), the target rows
 *                            into an source array.
 *
 * @param {Object[]}
 */
Combinator.prototype.join = function join(source, target, sourceCol, targetCol, opts) {
  var res = [], arrayAgg, util = vdba.util;

  //(1) pre: arguments
  arrayAgg = opts.arrayAgg;

  //(2) join
  for (var i = 0; i < source.length; ++i) {
    var srcRow = util._extend({}, source[i]);
    var arrAgg = srcRow[arrayAgg] = [];

    for (var j = 0; j < target.length; ++j) {
      var tgtRow = util._extend(target[j]);

      if (srcRow[sourceCol] == tgtRow[targetCol]) arrAgg.push(tgtRow);
    }

    res.push(srcRow);
  }

  //(3) return
  return res;
};

/**
 * A connection.
 *
 * @class vdba.Connection
 * @abstract
 *
 * @param {Object} config The configuration.
 */
function Connection(config) {
  /**
   * The configuration object.
   *
   * @name config
   * @type {Object}
   * @memberof vdba.Connection#
   */
  Object.defineProperty(this, "config", {value: config, enumerable: true});
}

/**
 * Returns a connection metadata ready to open.
 *
 * @name clone
 * @function
 * @memberof vdba.Connection#
 * @abstract
 */
Connection.prototype.clone = function clone() {
  throw new Error("Abstract method.");
};

/**
 * Is it connected?
 *
 * @name connected
 * @type {Boolean}
 * @memberof vdba.Connection#
 * @abstract
 */
Connection.prototype.__defineGetter__("connected", function() {
  throw new Error("Abstract property.");
});

/**
 * The server object as connected.
 *
 * @name server
 * @type {vdba.Server}
 * @memberof vdba.Connection#
 * @abstract
 */
Connection.prototype.__defineGetter__("server", function() {
  throw new Error("Abstract property.");
});

/**
 * Opens the connection.
 *
 * @name open
 * @function
 * @memberof vdba.Connection#
 * @abstract
 *
 * @param {Function} [callback] The function to call: fn(error, db).
 *
 * @example
 * cx.open(function(error, db) { ... });
 */
Connection.prototype.open = function open() {
  throw new Error("Abstract method.");
};

/**
 * Closes the connection.
 *
 * @name close
 * @function
 * @memberof vdba.Connection#
 * @abstract
 *
 * @param {Function} [callback] The function to call: fn(error).
 *
 * @example
 * cx.close();
 * cx.close(function(error) { ... });
 */
Connection.prototype.close = function close() {
  throw new Error("Abstract method.");
};

/**
 * Runs a function into a transaction.
 *
 * @name runTransaction
 * @function
 * @memberof vdba.Connection#
 * @abstract
 *
 * @param {String} mode         The transaction mode: readonly or readwrite.
 * @param {Function} op         The operation to run into a transaction.
 * @param {Function} [callback] The function to call: fn(error).
 *
 * @example
 * cx.runTransaction("readonly", function(db) { ... });
 * cx.runTransaction("readonly", function(db) { ... }, function(error) { ... });
 */
Connection.prototype.runTransaction = function runTransaction() {
  throw new Error("Abstract method.");
};

/**
 * A database.
 *
 * @class vdba.Database
 * @abstract
 */
function Database() {

}

/**
 * The database name.
 *
 * @name name
 * @type {String}
 * @memberof vdba.Database#
 * @abstract
 */
Database.prototype.__defineGetter__("name", function() {
  throw new Error("Abstract property.");
});

/**
 * Does the table exist?
 *
 * @name hasTable
 * @function
 * @memberof vdba.Database#
 * @abstract
 *
 * @param {String} name       The table name.
 * @param {Function} callback The function to call: fn(exists).
 *
 * @example
 * db.hasTable("user", function(error, exists) { ... });
 */
Database.prototype.hasTable = function hasTable() {
  throw new Error("Abstract method.");
};

/**
 * Do the tables exist?
 *
 * @name hasTables
 * @function
 * @memberof vdba.Database#
 * @abstract
 *
 * @param {String[]} names    The table names.
 * @param {Function} callback The function to call: fn(exist).
 *
 * @example
 * db.hasTables(["user", "session"], function(error, exist) { ... });
 */
Database.prototype.hasTables = function hasTables() {
  throw new Error("Abstract method.");
};

/**
 * Returns a table.
 *
 * @name findTable
 * @function
 * @memberof vdba.Database#
 * @abstract
 *
 * @param {String} name       The table name.
 * @param {Function} callback The function to call: fn(error, store).
 *
 * @example
 * db.findTable("user", function(error, store) { ... });
 */
Database.prototype.findTable = function findTable() {
  throw new Error("Abstract method.");
};

/**
 * Creates a new table.
 *
 * @name createTable
 * @function
 * @memberof vdba.Database#
 * @abstract
 *
 * @param {String} name         The table name.
 * @param {Object} [options]    The creation options.
 * @param {Function} [callback] The function to call: fn(error, table).
 */
Database.prototype.createTable = function createTable() {
  throw new Error("Abstract method.");
};

/**
 * Creates new tables.
 *
 * @name createTables
 * @function
 * @memberof vdba.Database#
 * @abstract
 *
 * @param {Object[]} tables     The tables info.
 * @param {Function} [callback] The function to call: fn(error, tables).
 */
Database.prototype.createTables = function createTables() {
  throw new Error("Abstract method.");
};

/**
 * Drops a table.
 *
 * @name dropTable
 * @function
 * @memberof vdba.Database#
 * @abstract
 *
 * @param {String} name         The table name.
 * @param {Function} [callback] The function to call: fn(error).
 */
Database.prototype.dropTable = function dropTable() {
  throw new Error("Abstract method.");
};

/**
 * Returns an index.
 *
 * @name findIndex
 * @function
 * @memberof vdba.Database#
 * @abstract
 *
 * @param {String} table      The table name.
 * @param {String} index      The index name.
 * @param {Function} callback The function to call: fn(error, index).
 *
 * @example
 * db.findIndex("user", "ix_username", function(error, ix) { ... });
 */
Database.prototype.findIndex = function findIndex() {
  throw new Error("Abstract method.");
};

/**
 * Checks whether a table has a specified index.
 *
 * @name hasIndex
 * @function
 * @memberof vdba.Database#
 * @abstract
 *
 * @param {String} table      The object store name.
 * @param {String} ix         The index name.
 * @param {Function} callback The function to call: fn(error, exist).
 */
Database.prototype.hasIndex = function hasIndex() {
  throw new Error("Abstract method.");
};

/**
 * Creates an index.
 *
 * @name createIndex
 * @function
 * @memberof vdba.Database#
 * @abstract
 *
 * @param {String} table        The table name.
 * @param {String} index        The index name.
 * @param {String} col          The indexing column.
 * @param {Object} [options]    The index options: unique (boolean).
 * @param {Function} [callback] The function to call: fn(error).
 *
 * @example
 * db.createIndex("user", "ix_username", "username");
 * db.createIndex("user", "ix_username", "username", function(error) { ... });
 * db.createIndex("user", "ix_username", "username", {unique: true});
 * db.createIndex("user", "ix_username", "username", {unique: true}, function(error) { ... });
 */
Database.prototype.createIndex = function createIndex() {
  throw new Error("Abstract method.");
};

/**
 * Drops an index.
 *
 * @name dropIndex
 * @function
 * @memberof vdba.Database#
 * @abstract
 *
 * @param {String} table        The table name.
 * @param {String} index        The index name.
 * @param {Function} [callback] The function to call: fn(error).
 *
 * @example
 * db.dropIndex("user", "ix_username");
 * db.dropIndex("user", "ix_username", function(error) { ... });
 */
Database.prototype.dropIndex = function dropIndex() {
  throw new Error("Abstract method.");
};

/**
 * A VDBA driver.
 *
 * @class vdba.Driver
 * @abstract
 *
 * @param {String} name The driver name.
 */
function Driver(name) {
  /**
   * The driver name.
   *
   * @name name
   * @type {String}
   * @memberof vdba.Driver#
   */
  Object.defineProperty(this, "name", {value: name, enumerable: true});
}

/**
 * The driver cache.
 *
 * @name cache
 * @type {Object}
 * @memberof vdba.Driver
 * @private
 */
Object.defineProperty(Driver, "cache", {value: {}});

/**
 * Returns a specified driver.
 *
 * @memberof vdba.Driver
 *
 * @param {String} name The driver name: IndexedDB, C*, Cassandra, PostgreSQL, etc.
 * @returns A driver or undefined if the name is invalid.
 *
 * @example
 * drv = vdba.Driver.getDriver("IndexedDB");
 * drv = vdba.Driver.getDriver("C*");
 */
Driver.getDriver = function getDriver(name) {
  var cache = vdba.Driver.cache;

  //(1) pre: arguments
  if (!name) {
    throw new Error("Driver name expected.");
  }

  //(2) return driver
  return cache[name.toLowerCase()];
};

/**
 * Registers a driver.
 * This method is used by the drivers to register in the VDBA API.
 *
 * @name register
 * @function
 * @memberof vdba.Driver
 *
 * @param {vdba.Driver} driver      The driver.
 * @param {String|String[]} [alias] The driver alias.
 *
 * @example
 * vdba.Driver.register(new IndexedDBDriver());
 * vdba.Driver.register(new CassandraDriver(), "C*");
 */
Driver.register = function register(driver, alias) {
  var cache = vdba.Driver.cache;

  //(1) pre: arguments
  if (!driver) {
    throw new Error("Driver expected.");
  }


  //(2) register
  cache[driver.name.toLowerCase()] = driver;

  if (alias) {
    if (typeof(alias) == "string") alias = [alias];

    for (var i = 0; i < alias.length; ++i) {
      cache[alias[i].toLowerCase()] = driver;
    }
  }
};

/**
 * Creates a connection object
 *
 * @name createConnection
 * @function
 * @memberof vdba.Driver#
 * @abstract
 *
 * @param {Object} config The connection configuration.
 * @returns {vdba.Connection}
 *
 * @example An IndexedDB connection.
 * cx = drv.createConnection({database: "mydb"});
 */
Driver.prototype.createConnection = function createConnection() {
  throw new Error("Abstract method.");
};

/**
 * Creates and opens a connection.
 *
 * @name openConnection
 * @function
 * @memberof vdba.Driver#
 *
 * @param {Object} config     The configuration object.
 * @param {Function} callback The function to call: fn(error, cx).
 *
 * @example An IndexedDB connection.
 * drv.openConnection({database: "mydb"}, function(error, cx) { ... });
 */
Driver.prototype.openConnection = function openConnection(config, callback) {
  var cx;

  //(1) pre: arguments
  if (!config) {
    throw new Error("Configuration expected.");
  }

  if (!callback) {
    throw new Error("Callback expected.");
  }

  //(2) create connection
  cx = this.createConnection(config);

  //(3) open connection
  cx.open(function(error) {
    if (error) callback(error);
    else callback(undefined, cx);
  });
};

/**
 * An index.
 *
 * @class vdba.Index
 * @abstract
 */
function Index() {

}

/**
 * The database.
 *
 * @name table
 * @type {vdba.Database}
 * @memberof vdba.Index#
 * @abstract
 */
Index.prototype.__defineGetter__("database", function() {
  throw new Error("Abstract property.");
});

/**
 * The table.
 *
 * @name table
 * @type {vdba.Table}
 * @memberof vdba.Index#
 * @abstract
 */
Index.prototype.__defineGetter__("table", function() {
  throw new Error("Abstract property.");
});

/**
 * The index name.
 *
 * @name name
 * @type {String}
 * @memberof vdba.Index#
 * @abstract
 */
Index.prototype.__defineGetter__("name", function() {
  throw new Error("Abstract property.");
});

/**
 * Is it unique?
 *
 * @name unique
 * @type {Boolean}
 * @memberof vdba.indexeddb.IndexedDBIndex#
 * @abstract
 */
Index.prototype.__defineGetter__("unique", function() {
  throw new Error("Abstract property.");
});

/**
 * A mapper.
 *
 * @class vdba.Mapper
 * @protected
 */
function Mapper() {

}

/**
 * Maps the rows as indicated.
 *
 * @name map
 * @function
 * @memberof vdba.Mapper#
 *
 * @param {Object|String[]|Function} map  How to map.
 * @param {vdba.Result|Object[]} rows     The rows or the result to cast.
 *
 * @returns {Object[]}
 */
Mapper.prototype.map = function(map, rows) {
  var res;

  //(1) map
  if (rows instanceof vdba.Result) {
    res = this.mapResult(map, rows);
  } else {
    res = this.mapRows(map, rows);
  }

  //(2) return result
  return res;
};

/**
 * @private
 */
Mapper.prototype.mapRows = function(map, rows) {
  var res = [];

  //(1) map
  for (var i = 0; i < rows.length; ++i) {
    res.push(this.mapRow(map, rows[i]));
  }

  //(2) return result
  return res;
};

/**
 * @private
 */
Mapper.prototype.mapResult = function(map, result) {
  var res;

  //(1) create result
  res = new result.constructor([], result.options);

  //(2) map
  for (var i = 0; i < result.length; ++i) {
    res.rows.push(this.mapRow(map, result.rows[i]));
  }

  //(3) return result
  return res;
};

/**
 * Maps a row.
 *
 * @name mapRow
 * @function
 * @memberof vdba.Mapper#
 *
 * @param {Object|String[]|Function} map  How to map.
 * @param {Object} row                    The row to cast.
 *
 * @returns {Object}
 */
Mapper.prototype.mapRow = function(map, row) {
  var instance;

  //(1) cast as needed
  if (map instanceof Function) {
    instance = this.customMap(map, row);
  } else if (map instanceof Array) {
    instance = this.defaultMap({map: map}, row);
  } else if (map instanceof Object) {
    instance = this.defaultMap(map, row);
  } else {
    instance = row;
  }

  //(2) return instance
  return instance;
};

/**
 * @private
 */
Mapper.prototype.defaultMap = function(map, row) {
  var instance, Class, mapping;

  //(1) prepare
  Class = (map.clss || Object);
  mapping = (map.map || {});

  if (typeof(mapping) == "string") {
    mapping = [mapping];
  }

  if (mapping instanceof Array) {
    var aux = {};

    for (var i = 0; i < mapping.length; ++i) {
      var field = mapping[i];
      aux[field.toLowerCase()] = field;
    }

    mapping = aux;
  }

  //(2) create instance
  instance = Object.create(Class.prototype);

  //(3) initialize instance
  for (var key in row) {
    instance[mapping[key] || key] = row[key];
  }

  //(4) return instance
  return instance;
};

/**
 * @private
 */
Mapper.prototype.customMap = function(map, row) {
  return map(row);
};

/**
 * A query.
 *
 * @class vdba.Query
 * @abstract
 */
function Query() {

}

/**
 * Returns all records.
 *
 * @name findAll
 * @function
 * @memberof vdba.Query#
 * @abstract
 *
 * @param {Function} callback The function to call: fn(error, result).
 */
Query.prototype.findAll = function findAll() {
  throw new Error("Abstract method.");
};

/**
 * findAll() with casting.
 *
 * @name mapAll
 * @function
 * @memberof vdba.Query#
 *
 * @param {Object|Function|String[]} map  The mapping.
 * @param {Function} callback             The function to call: fn(error, result).
 *
 * @example
 * q.mapAll(["userId"], function(error, result) { ... });
 * q.mapAll({clss: User}, function(error, result) { ... });
 * q.mapAll({clss: User, map: ["userId"]}, function(error, result) { ... });
 * q.mapAll({clss: User, map: {userid: "userId"}}, function(error, result) { ... });
 */
Query.prototype.mapAll = function mapAll(map, callback) {
  //(1) pre: arguments
  if (!map) throw new Error("Map expected.");
  if (!callback) throw new Error("Callback expected.");

  //(2) find and map
  this.findAll(function(error, result) {
    if (error) callback(error);
    else callback(undefined, new vdba.Mapper().map(map, result));
  });
};

/**
 * Runs the query.
 *
 * @name find
 * @function
 * @memberof vdba.Query#
 * @abstract
 *
 * @param {Object} [filter]   The filter object.
 * @param {Function} callback The function to call: fn(error, result).
 */
Query.prototype.find = function find() {
  throw new Error("Abstract method.");
};

/**
 * find() with casting.
 *
 * @name map
 * @function
 * @memberof vdba.Query#
 *
 * @param {Object|Function|String[]} map  The mapping.
 * @param {Object} [filter]               The condition.
 * @param {Function} callback             The function to call: fn(error, result).
 */
Query.prototype.map = function(map, filter, callback) {
  //(1) pre: arguments
  if (arguments.length == 2) {
    if (arguments[1] instanceof Function) {
      callback = arguments[1];
      filter = undefined;
    }
  }

  if (!map) throw new Error("Map expected.");
  if (!callback) throw new Error("Callback expected.");

  //(2) find and map
  this.find(filter, function(error, result) {
    if (error) {
      callback(error);
    } else {
      callback(undefined, new vdba.Mapper().map(map, result));
    }
  });
};

/**
 * Runs the query.
 *
 * @name findOne
 * @function
 * @memberof vdba.Query#
 * @abstract
 *
 * @param {Object} [filter]   The filter object.
 * @param {Function} callback The function to call: fn(error, record).
 */
Query.prototype.findOne = function findOne() {
  throw new Error("Abstract method.");
};

/**
 * findOne() with casting.
 *
 * @name mapOne
 * @function
 * @memberof vdba.Query#
 *
 * @param {Object|Function|String[]} map  The mapping.
 * @param {Object} [filter]               The filter object.
 * @param {Function} callback             The function to call: fn(error, record).
 */
Query.prototype.mapOne = function mapOne(map, filter, callback){
  //(1) pre: arguments
  if (arguments.length == 2) {
    if (arguments[1] instanceof Function) {
      callback = arguments[1];
      filter = undefined;
    }
  }

  if (!map) throw new Error("Map expected.");
  if (!callback) throw new Error("Callback expected.");

  //(2) find and map
  this.findOne(filter, function(error, row) {
    if (error) {
      callback(error);
    } else {
      if (!row) callback();
      else callback(undefined, new vdba.Mapper().mapRow(map, row));
    }
  });
};

/**
 * Performs a join.
 *
 * @name join
 * @function
 * @memberof vdba.Query#
 * @abstract
 *
 * @param {String|vdba.Table} target  The target table.
 * @param {String} col1               The source column.
 * @param {String} [col2]             The target column.
 * @param {Function} [callback]       The function to call: function(error, result).
 *
 * @returns {vdba.Query} If no callback is specified, it returns the query.
 *
 * @example Natural join.
 * q.join("session", "userId")
 * q.join("session", "userId", function(error, result) { ... });
 *
 * @example
 * q.join("session", "userId", "userId")
 * q.join("session", "userId", "userId", function(error, result) { ... });
 *
 * @example Restricting records.
 * q.join("session", "userId").find({username: "user01"}, function(error, result) { ... });
 */
Query.prototype.join = function join() {
  throw new Error("Abstract method.");
};

/**
 * A query result.
 *
 * @class vdba.Result
 *
 * @param {Array} rows        The rows.
 * @param {Object} [options]  The options.
 */
function Result(rows, options) {
  /**
   * The rows.
   *
   * @name rows
   * @type {Object[]}
   * @memberof vdba.Result#
   */
  Object.defineProperty(this, "rows", {value: rows});

  /**
   * The result options.
   *
   * @name options
   * @type {Object}
   * @memberof vdba.Result#
   */
  Object.defineProperty(this, "options", {value: options || {}});
}

/**
 * The number of records.
 *
 * @name length
 * @type {Number}
 * @memberof vdba.Result#
 */
Result.prototype.__defineGetter__("length", function() {
  return this.rows.length;
});

/**
 * Returns the rows satisfying the restriction.
 *
 * @name find
 * @function
 * @memberof vdba.Result#
 *
 * @param {Object} [where]  The restriction condition.
 *
 * @returns {Object[]}
 */
Result.prototype.find = function find(where) {
  return new vdba.ResultFilter().find(this, where);
};

/**
 * Returns the rows satisfying the restriction casted as
 * indicated.
 *
 * @name map
 * @function
 * @memberof vdba.Result#
 *
 * @param {Object} map      The mapping.
 * @param {Object} [where]  The restriction condition.
 *
 * @returns {Object[]}
 */
Result.prototype.map = function(map, where) {
  return new vdba.Mapper().map(map, this.find(where));
};

/**
 * A result filter.
 *
 * @class vdba.ResultFilter
 * @private
 */
function ResultFilter() {

}

/**
 * Filters rows of a result.
 *
 * @memberof vdba.ResultFilter#
 *
 * @param {Result} result The result set.
 * @param {Object} filter The filter.
 *
 * @returns {Object[]}
 */
ResultFilter.prototype.find = function find(result, filter) {
  var filtered = [];

  //(1) arguments
  if (!filter) filter = {};

  //(3) filter
  for (var i = 0, rows = result.rows; i < result.length; ++i) {
    var row = rows[i];

    if (this.check(row, filter)) filtered.push(row);
  }

  //(3) retun result
  return filtered;
};

/**
 * Checks whether a row satifies the filter.
 *
 * @memberof vdba.ResultFilter#
 *
 * @param {Object} row    The row to check.
 * @param {Object} filter The filter.
 *
 * @returns {Boolean}
 */
ResultFilter.prototype.check = function check(row, filter) {
  var res = false, keys = Object.keys(filter);

  //(1) check
  if (keys.length === 0) {             //{}
    res = true;
  } else if (keys.length == 1) {      //{prop: ...}
    res = this.checkProp(row, keys[0], filter);
  } else {                            //{prop1: ..., prop2: ...}
    res = true;

    for (var i = 0, props = keys; i < props.length; ++i) {
      var prop = props[i];

      if (!this.checkProp(row, prop, filter)) {
        res = false;
        break;
      }
    }
  }

  //(2) return result
  return res;
};

/**
 * Checks whether a property satisfies its filter.
 *
 * @memberof vdba.ResultFilter#
 * @private
 *
 * @param {Object} row    The row to check.
 * @param {String} prop   The property to check.
 * @param {Object} filter The filter.
 *
 * @returns {Boolean}
 */
ResultFilter.prototype.checkProp = function checkProp(row, prop, filter) {
  var res;

  //(1) get property filter
  filter = filter[prop];

  //(2) check
  if (typeof(filter) != "object") {     //{prop: value}
    res = this.$eq(row, prop, filter);
  } else {                              //{prop: {...}}
    var ops = Object.keys(filter);

    if (ops.length === 0) {              //{prop: {}}
      res = true;
    } else if (ops.length == 1) {       //{prop: {op: value}
      res = this.checkOp(row, prop, ops[0], filter);
    } else {                           //{prop: {op1: value, opt2: value}}
      res = true;

      for (var i = 0; i < ops.length; ++i) {
        if (!this.checkOp(row, prop, ops[i], filter)) {
          res = false;
          break;
        }
      }
    }
  }

  //(3) return result
  return res;
};

/**
 * Checks a property with an operator.
 *
 * @memberof vdba.ResultFilter#
 * @private
 *
 * @param {Object} row    The row to check.
 * @param {String} prop   The property to check.
 * @param {String} op     The operator.
 * @param {Object} filter The filter.
 *
 * @returns {Boolean}
 */
ResultFilter.prototype.checkOp = function checkOp(row, prop, op, filter) {
  var res;

  //(1) check
  if (op == "$eq") res = this.$eq(row, prop, filter.$eq);
  else if (op == "$ne") res = this.$ne(row, prop, filter.$ne);
  else if (op == "$lt") res = this.$lt(row, prop, filter.$lt);
  else if (op == "$le") res = this.$le(row, prop, filter.$le);
  else if (op == "$gt") res = this.$gt(row, prop, filter.$gt);
  else if (op == "$ge") res = this.$ge(row, prop, filter.$ge);
  else if (op == "$like") res = this.$like(row, prop, filter.$like);
  else if (op == "$notLike") res = this.$notLike(row, prop, filter.$notLike);
  else if (op == "$in") res = this.$in(row, prop, filter.$in);
  else if (op == "$notIn") res = this.$notIn(row, prop, filter.$notIn);
  else throw new Error("Unknown operator: '" + op + "'.");

  //(2) return check
  return res;
};

/**
 * Checks the operator $eq.
 *
 * @memberof vdba.ResultFilter#
 * @private
 *
 * @param {Object} row    The row to check.
 * @param {String} prop   The property name to check.
 * @param {Object} value  The value to check.
 *
 * @returns {Boolean}
 */
ResultFilter.prototype.$eq = function $eq(row, prop, value) {
  if (value === undefined) return (row[prop] === undefined);
  else if (value === null) return (row[prop] === null);
  else return (row[prop] == value);
};

/**
 * Checks the operator $ne.
 *
 * @memberof vdba.ResultFilter#
 * @private
 *
 * @param {Object} row    The row to check.
 * @param {String} prop   The property to check.
 * @param {Object} value  The value to check.
 *
 * @returns {Boolean}
 */
ResultFilter.prototype.$ne = function $ne(row, prop, value) {
  if (value === undefined) return (row[prop] !== undefined);
  else if (value === null) return (row[prop] !== null);
  else return (row[prop] != value);
};

/**
 * Checks the operator $lt.
 *
 * @memberof vdba.ResultFilter#
 * @private
 *
 * @param {Object} row    The row to check.
 * @param {String} prop   The property to check.
 * @param {Object} value  The value to check.
 *
 * @returns {Boolean}
 */
ResultFilter.prototype.$lt = function $lt(row, prop, value) {
  if (value === undefined || value === null) return false;
  else return (row[prop] < value);
};

/***
 * Checks the operator $le.
 *
 * @memberof vdba.ResultFilter#
 * @private
 *
 * @param {Object} row    The row to check.
 * @param {String} prop   The property to check.
 * @param {Object} value  The value to check.
 *
 * @returns {Boolean}
 */
ResultFilter.prototype.$le = function $le(row, prop, value) {
  if (value === undefined || value === null) return false;
  else return (row[prop] <= value);
};

/**
 * Checks the operator $gt.
 *
 * @memberof vdba.ResultFilter#
 * @private
 *
 * @param {Object} row    The row to check.
 * @param {String} prop   The property to check.
 * @param {Object} value  The value to check.
 *
 * @returns {Boolean}
 */
ResultFilter.prototype.$gt = function $gt(row, prop, value) {
  if (value === undefined || value === null) return false;
  else return (row[prop] > value);
};

/**
 * Checks the operator $ge.
 *
 * @memberof vdba.ResultFilter#
 * @private
 *
 * @param {Object} row    The row to check.
 * @param {String} prop   The property to check.
 * @param {Object} value  The value to check.
 *
 * @returns {Boolean}
 */
ResultFilter.prototype.$ge = function $ge(row, prop, value) {
  if (value === undefined || value === null) return false;
  else return (row[prop] >= value);
};

/**
 * Checks the operator $like.
 *
 * @memberof vdba.ResultFilter#
 * @private
 *
 * @param {Object} row    The row to check.
 * @param {String} prop   The property to check.
 * @param {Object} value  The pattern to check.
 *
 * @returns {Boolean}
 */
ResultFilter.prototype.$like = function $like(row, prop, value) {
  if (value === undefined || value === null) return this.$eq(row, prop, value);
  else return new RegExp(value).test(row[prop]);
};

/**
 * Checks the operator $notLike.
 *
 * @memberof vdba.ResultFilter#
 * @private
 *
 * @param {Object} row    The row to check.
 * @param {String} prop   The property to check.
 * @param {Object} value  The pattern to check.
 *
 * @returns {Boolean}
 */
ResultFilter.prototype.$notLike = function $notLike(row, prop, value) {
  if (value === undefined || value === null) return this.$ne(row, prop, value);
  else return !this.$like(row, prop, value);
};

/**
 * Checks the operator $in.
 *
 * @memberof vdba.ResultFilter#
 * @private
 *
 * @param {Object} row    The row to check.
 * @param {String} prop   The property to check.
 * @param {Object} value  The values to check.
 *
 * @retuns {Boolean}
 */
ResultFilter.prototype.$in = function $in(row, prop, value) {
  if (value === undefined || value === null) return false;
  else return (value.indexOf(row[prop]) >= 0);
};

/**
 * Checks the operator $notIn.
 *
 * @memberof vdba.ResultFilter#
 * @private
 *
 * @param {Object} row    The row to check.
 * @param {String} prop   The property to check.
 * @param {Object} value  The value to check.
 *
 * @returns {Boolean}
 */
ResultFilter.prototype.$notIn = function $notIn(row, prop, value) {
  return !this.$in(row, prop, value);
};

/**
 * A database engine.
 *
 * @class vdba.Server
 * @abstract
 */
function Server() {

}

/**
 * The hostname.
 *
 * @name host
 * @memberof vdba.Server#
 * @abstract
 */
Server.prototype.__defineGetter__("host", function() {
  throw new Error("Abstract method.");
});

/**
 * The port.
 *
 * @name port
 * @memberof vdba.Server#
 * @abstract
 */
Server.prototype.__defineGetter__("port", function() {
  throw new Error("Abstract method.");
});

/**
 * The server version.
 *
 * @name version
 * @memberof vdba.Server#
 * @abstract
 */
Server.prototype.__defineGetter__("version", function() {
  throw new Error("Abstract method.");
});

/**
 * Creates a new database.
 *
 * @name createDatabase
 * @function
 * @memberof vdba.Server#
 * @abstract
 *
 * @param {String} name         The database name.
 * @param {Object} [options]    The database options.
 * @param {Function} [callback] The function to call: fn(error).
 */
Server.prototype.createDatabase = function createDatabase() {
  throw new Error("Abstract method.");
};

/**
 * Checks whether a database exists.
 *
 * @name hasDatabase
 * @function
 * @memberof vdba.Server#
 * @abstract
 *
 * @param {String} name       The database name.
 * @param {Function} callback The function to call: fn(error, exists).
 */
Server.prototype.hasDatabase = function hasDatabase() {
  throw new Error("Abstract method.");
};

/**
 * Drops a database.
 *
 * @name dropDatabase
 * @function
 * @memberof vdba.Server#
 * @abstract
 *
 * @param {String} name         The database name.
 * @param {Function} [callback] The function to call: fn(error).
 */
Server.prototype.dropDatabase = function dropDatabase() {
  throw new Error("Abstract method.");
};

/**
 * A table.
 *
 * @class vdba.Table
 * @abstract
 */
function Table() {

}

/**
 * The database object.
 *
 * @name database
 * @type {vdba.Database}
 * @memberof vdba.Table#
 * @abstract
 */
Table.prototype.__defineGetter__("database", function() {
  throw new Error("Abstract method.");
});

/**
 * The table name.
 *
 * @name name
 * @type {String}
 * @memberof vdba.Table#
 * @abstract
 */
Table.prototype.__defineGetter__("name", function() {
  throw new Error("Abstract method.");
});

/**
 * Checks whether an index exists.
 *
 * @name hasIndex
 * @function
 * @memberof vdba.Table#
 *
 * @param {String} name       The index name.
 * @param {Function} callback The function to call: fn(error, exists).
 */
Table.prototype.hasIndex = function hasIndex(name, callback) {
  //(1) arguments
  if (!name) {
    throw new Error("Index name expected.");
  }

  if (!callback) {
    throw new Error("Callback expected.");
  }

  //(2) check
  this.database.hasIndex(this.name, name, callback);
};

/**
 * Returns an index.
 *
 * @name findIndex
 * @function
 * @memberof vdba.Table#
 *
 * @param {String} name       The index name.
 * @param {Function} callback The function to call: fn(error, exists).
 */
Table.prototype.findIndex = function findIndex(name, callback) {
  //(1) arguments
  if (arguments.length < 2) {
    throw new Error("Index name and callback expected.");
  }

  //(2) find
  this.database.findIndex(this.name, name, callback);
};

/**
 * Creates an index on the table.
 *
 * @name createIndex
 * @function
 * @memberof vdba.Table#
 *
 * @param {String} name         The index name.
 * @param {String} col          The column.
 * @param {Object} [options]    The index options: unique (boolean).
 * @param {Function} [callback] The function to call: fn(error).
 *
 * @example Create a unique index
 * tab.createIndex("ix_username", "username", {unique: true});
 * tab.createIndex("ix_username", "username", {unique: true}, function(error) { ... });
 */
Table.prototype.createIndex = function createIndex(name, col, options, callback) {
  //(1) arguments
  if (arguments.length < 2) {
    throw new Error("Index name and indexing column expected.");
  } else if (arguments.length == 3) {
    if (arguments[2] instanceof Function) {
      callback = arguments[2];
      options = {};
    }
  }

  //(2) create
  this.database.createIndex(this.name, name, col, options, callback);
};

/**
 * Drops an index.
 *
 * @name dropIndex
 * @function
 * @memberof vdba.Table#
 *
 * @param {String} name         The index name.
 * @param {Function} [callback] The function to call: fn(error).
 *
 * @example Drop an index
 * tab.drop("ix_username");
 * tab.drop("ix_username", function(error) { ... });
 */
Table.prototype.dropIndex = function dropIndex(name, callback) {
  //(1) arguments
  if (arguments.length < 1) {
    throw new Error("Index name expected.");
  }

  //(2) drop
  this.database.dropIndex(this.name, name, callback);
};

/**
 * Returns a query object.
 *
 * @name query
 * @function
 * @memberof vdba.Table#
 * @protected
 * @abstract
 *
 * @returns {vdba.Query}
 */
Table.prototype.query = function query() {
  throw new Error("Abstract method.");
};

/**
 * Returns zero, one or several rows.
 *
 * @name find
 * @function
 * @memberof vdba.Table#
 *
 * @param {Object} filter     The condition.
 * @param {Function} callback The function to call: fn(error, result).
 */
Table.prototype.find = function find(filter, callback) {
  //(1) pre: arguments
  if (arguments.length == 1) {
    callback = arguments[0];
    filter = undefined;
  }

  if (!callback) {
    throw new Error("Callback expected.");
  }

  //(2) find
  this.query().find(filter, callback);
};

/**
 * find() with casting.
 *
 * @name map
 * @function
 * @memberof vdba.Table#
 *
 * @param {Object|Function|String[]} map  The mapping.
 * @param {Object} [filter]               The condition.
 * @param {Function} callback             The function to call: fn(error, result).
 */
Table.prototype.map = function(map, filter, callback) {
  this.query().map(map, filter, callback);
};

/**
 * Returns all rows.
 *
 * @name findAll
 * @function
 * @memberof vdba.Table#
 *
 * @param {Function} callback The function to call: fn(error, result).
 */
Table.prototype.findAll = function findAll(callback) {
  //(1) pre: arguments
  if (!callback) throw new Error("Callback expected.");

  //(2) find
  this.query().find(callback);
};

/**
 * findAll() with casting.
 *
 * @name mapAll
 * @function
 * @memberof vdba.Table#
 *
 * @param {Object|Function|String[]} map  The mapping.
 * @param {Function} callback             The function to call: fn(error, result).
 */
Table.prototype.mapAll = function mapAll(map, callback) {
  this.query().mapAll(map, callback);
};

/**
 * Returns one row.
 *
 * @name findOne
 * @function
 * @memberof vdba.Table#
 *
 * @param {Object} [filter]   The condition.
 * @param {Function} callback The function to call: fn(error, row).
 */
Table.prototype.findOne = function findOne(filter, callback) {
  //(1) pre: arguments
  if (arguments.length == 1) {
    callback = arguments[0];
    filter = undefined;
  }

  //(2) find
  this.query().findOne(filter, callback);
};

/**
 * findOne() with casting.
 *
 * @name mapOne
 * @function
 * @memberof vdba.Table#
 *
 * @param {Object|Function|String[]} map  The mapping.
 * @param {Object} [filter]               The condition.
 * @param {Function} callback             The function to call: fn(error, row).
 */
Table.prototype.mapOne = function mapOne(map, filter, callback) {
  this.query().mapOne(map, filter, callback);
};

/**
 * Returns the number of rows.
 *
 * @name count
 * @function
 * @memberof vdba.Table#
 * @abstract
 *
 * @param {Function} callback The function to call: fn(error, count).
 */
Table.prototype.count = function count() {
  throw new Error("Abstract method.");
};

/**
 * Joins this table with another.
 *
 * @name join
 * @function
 * @memberof vdba.Table#
 *
 * @param {String|vdba.Table} target  The target table name.
 * @param {String} col1               The source column.
 * @param {String} [col2]             The target column.
 * @param {Function} [callback]       The function to call: fn(error, result).
 *
 * @returns {vdba.Query} If the call doesn't pass a callback, it returns a Query;
 *                       otherwise, asynchronous call.
 */
Table.prototype.join = function join(target, col1, col2, callback) {
  //(1) pre: arguments
  if (arguments.length == 3) {
    if (arguments[2] instanceof Function) {
      callback = arguments[2];
      col2 = undefined;
    }
  }

  if (!col2) col2 = col1;

  if (!target) throw new Error("Target table expected.");
  if (!col1) throw new Error("Source column expected.");
  if (!col2) throw new Error("Target column expected.");

  //(2) join or return
  if (callback) {
    this.query().join(target, col1, col2, callback);
  } else {
    return this.query().join(target, col1, col2);
  }
};

/**
 * Inserts one or several rows into the table.
 *
 * @name insert
 * @function
 * @memberof vdba.Table#
 * @abstract
 *
 * @param {object|Object[]} rows  The row(s) to insert.
 * @param {Function} [callback]   The function to call: fn(error).
 */
Table.prototype.insert = function insert() {
  throw new Error("Abstract method.");
};

/**
 * Replaces the content of one or several rows.
 * The record must exist.
 *
 * @name save
 * @function
 * @memberof vdba.Table#
 * @abstract
 *
 * @param {Object|Object[]} rows  The row(s) to save.
 * @param {Function} [callback]   The function to call: fn(error).
 *
 * @example
 * user.save({userId: 1, username: "user01", password: "pwd01"});
 * user.save([{...}, {...}, {...}], function(error) { ... });
 */
Table.prototype.save = function save() {
  throw new Error("Abstract method.");
};

/**
 * Updates zero, one or several rows.
 *
 * @name update
 * @function
 * @memberof vdba.Table#
 * @abstract
 *
 * @param {Object} [where]      The condition.
 * @param {Object} cols         The columns to update.
 * @param {Function} [callback] The function to call: fn(error).
 *
 * @example
 * user.update({state: "locked"}, function(error) { ... });
 * user.update({userId: 1}, {password: "newPwd"}, function(error) { ... });
 */
Table.prototype.update = function update() {
  throw new Error("Abstract method.");
};

/**
 * Removes zero, one or several rows.
 *
 * @name remove
 * @function
 * @memberof vdba.Table#
 * @abstract
 *
 * @param {Object} where        The condition.
 * @param {Function} [callback] The function to call: fn(error).
 *
 * @example TRUNCATE
 * user.remove(function(error) { ... });
 *
 * @example DELETE
 * user.remove({userId: 1}, function(error) { ... });
 */
Table.prototype.remove = function remove() {
  throw new Error("Abstract method.");
};

//imports
var util = require("util");

//api
var vdba = module.exports;

/**
 * The vdba package.
 *
 * @namespace vdba
 */
Object.defineProperty(vdba, "util", {
  value: {
    inherits: util.inherits,
    _extend: util._extend
  }
});

Object.defineProperty(vdba, "Combinator", {value: Combinator, enumerable: true});
Object.defineProperty(vdba, "Connection", {value: Connection, enumerable: true});
Object.defineProperty(vdba, "Database", {value: Database, enumerable: true});
Object.defineProperty(vdba, "Driver", {value: Driver, enumerable: true});
Object.defineProperty(vdba, "Index", {value: Index, enumerable: true});
Object.defineProperty(vdba, "Mapper", {value: Mapper, enumerable: true});
Object.defineProperty(vdba, "Query", {value: Query, enumerable: true});
Object.defineProperty(vdba, "Result", {value: Result, enumerable: true});
Object.defineProperty(vdba, "ResultFilter", {value: ResultFilter, enumerable: true});
Object.defineProperty(vdba, "Server", {value: Server, enumerable: true});
Object.defineProperty(vdba, "Table", {value: Table, enumerable: true});

})();