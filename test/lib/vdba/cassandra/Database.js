describe("vdba.cassandra.CassandraDatabase", function() {
  var drv;

  before(function() {
    drv = vdba.Driver.getDriver(config.driver.name);
  });

  describe("Properties", function() {
    var cx;

    before(function(done) {
      drv.openConnection(config.connection.config, function(error, con) {
        cx = con;
        done();
      });
    });

    after(function(done) {
      cx.close(done);
    });

    it("connection", function() {
      cx.database.connection.should.be.exactly(cx);
    });

    it("name", function() {
      cx.database.name.should.be.eql(config.connection.config.database);
    });
  });
});