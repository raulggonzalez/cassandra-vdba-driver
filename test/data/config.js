exports.driver = {
  name: "Cassandra",
  alias: ["C*"]
};

exports.connection = {
  config: {
    hosts: ["localhost"],
    port: 9042,
    database: "odba"
  }
};