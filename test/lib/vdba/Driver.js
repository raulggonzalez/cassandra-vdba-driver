describe("vdba.Driver", function() {
  var drv;

  before(function() {
    drv = vdba.Driver.getDriver(config.driver.name);
  });

  describe("#createConnection()", function() {
    describe("Error handling", function() {
      it("createConnection({})", function() {
        (function() {
          drv.createConnection({});
        }).should.throwError("Database expected.");
      });

      it("createConnection(config) - Unknown read consistency", function() {
        (function() {
          drv.createConnection({database: config.connection.config.database, readConsistency: "unknown"});
        }).should.throwError("Unknown consistency: unknown.");
      });

      it("createConnection(config) - Unknown write consistency", function() {
        (function() {
          drv.createConnection({database: config.connection.config.database, writeConsistency: "unknown"});
        }).should.throwError("Unknown consistency: unknown.");
      });
    });
  });
});