describe("vdba.cassandra.CassandraServer", function() {
  var drv, cx;

  before(function() {
    drv = vdba.Driver.getDriver(config.driver.name);
  });

  before(function(done) {
    drv.openConnection(config.connection.config, function(error, con) {
      cx = con;
      done();
    });
  });

  after(function(done) {
    cx.close(done);
  });

  describe("Properties", function() {
    it("connection", function() {
      cx.server.connection.should.be.exactly(cx);
    });

    it("host", function() {
      cx.server.host.should.be.eql("localhost");
    });

    it("port", function() {
      cx.server.port.should.be.eql(config.connection.config.port);
    });

    it("version", function() {
      (function() {
        cx.server.version.should.be.eql("2.1.2");
      }).should.throwError("Unsupported property.");
    });
  });
});