//imports
var vdba = require("./node-vdba-core");

//api
for (var i=0, keys = Object.keys(vdba); i < keys.length; ++i) {
  var key = keys[i];
  Object.defineProperty(exports, key, {value: vdba[key]});
}

/**
 * The C* VDBA namespace.
 *
 * @namespace vdba.cassandra
 */
Object.defineProperty(exports, "cassandra", {
  value: {},
  enumerable: true
});

//register driver
vdba.Driver.register(new (require("./vdba/cassandra/Driver").CassandraDriver)());