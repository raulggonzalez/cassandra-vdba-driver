describe("vdba.cassandra.CassandraDatabase", function() {
  var user = config.database.user;
  var drv, cx, db;

  before(function() {
    drv = vdba.Driver.getDriver(config.driver.name);
  });

  beforeEach(function(done) {
    drv.openConnection(config.connection.config, function(error, con) {
      cx = con;
      db = cx.database;
      done();
    });
  });

  afterEach(function(done) {
    cx.close(done);
  });

  describe("Properties", function() {
    it("connection", function() {
      db.connection.should.be.exactly(cx);
    });

    it("name", function() {
      db.name.should.be.eql(config.connection.config.database);
    });
  });

  describe("#createTable()", function() {
    afterEach(function(done) {
      db.dropTable(user.name, done);
    });

    describe("Error handling", function() {
      it("createTable(name)", function() {
        (function() {
          db.createTable(user.name);
        }).should.throwError("Table columns expected.");
      });

      it("createTable(name, {})", function() {
        (function() {
          db.createTable(user.name, {});
        }).should.throwError("Table columns expected.");
      });
    });
  });
});