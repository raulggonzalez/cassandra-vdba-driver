exports.driver = {
  name: "Cassandra",
  alias: ["C*"]
};

exports.connection = {
  config: {
    hosts: ["localhost"],
    port: 9042,
    database: "odba",
    username: "vdba",
    password: "vdba123."
  }
};

exports.database = {
  user: {
    name: "User",
    columns: {
      userId: {type: "int", pk: true},
      username: "text",
      password: "text"
    },
    index: {
      name: "ix_user_username",
      columns: "username",
      options: {}
    }
  },

  session: {
    name: "Session",
    columns: {
      sessionId: {type: "int", pk: true},
      userId: "int"
    }
  }
};