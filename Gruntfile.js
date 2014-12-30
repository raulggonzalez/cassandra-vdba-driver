//imports
var child_process = require("child_process");

//Grunt
module.exports = function(grunt) {
  //(1) configure
  grunt.config.init({
    pkg: grunt.file.readJSON("package.json"),

    clean: {
      doc: {
        src: ["doc/api/"]
      }
    },

    concat: {
      options: {
        separator: "\n\n"
      },

      node: {
        options: {
          banner: "/*! <%= pkg.name %> - <%= pkg.version %> (<%= grunt.template.today('yyyy-mm-dd') %>) */\n\n(function() {",
          footer: "\n\n})();"
        },

        src: ["lib/**"],
        dest: "cassandra-vdba-driver.js"
      }
    },

    compress: {
      "api.html": {
        options: {
          mode: "zip",
          archive: "doc/api.html.zip",
          level: 3,
        },

        expand: true,
        cwd: "doc/api/",
        src: "**",
      }
    },

    jsdoc: {
      "api.html": {
        src: ["cassandra-vdba-driver.js"],
        options: {
          recurse: true,
          template: "templates/default",
          destination: "doc/api",
          "private": false
        }
      }
    },

    jshint: {
      grunt: {
        files: {
          src: ["Gruntfile.js"]
        }
      },

      lib: {
        options: {
          jshintrc: true
        },

        files: {
          src: ["lib/**"]
        }
      },

      node: {
        options: {
          jshintrc: true
        },

        files: {
          src: ["cassandra-vdba-driver.js"]
        }
      },

      test: {
        options: {
          ignores: [
            "test/data/*.cql",
            "test/vendor/**",
            "test/mocha.opts"
          ]
        },

        files: {
          src: ["test/**"]
        }
      }
    },

    mochaTest:{
      options: {
        ignoreLeaks: false,
        quiet: false,
        reporter: "spec",
        require: ["should", function() { util = require("util"); }]
      },

      common: {
        src: ["test/init.js", "../vdba-driver-validator/test/common/**/*.js"]
      },

      specific: {
        src: ["test/init.js", "test/**/*.js"]
      }
    },

    uglify: {
      options: {
        banner: "/*! <%= pkg.name %> - <%= pkg.version %> (<%= grunt.template.today('yyyy-mm-dd') %>) */",
        mangle: false,
        compress: {
          drop_console: true,
          drop_debugger: true,
          properties: true,
          dead_code: true,
          conditionals: true,
          comparisons: true,
          booleans: true,
          loops: true,
          warnings: true
        },
        preserveComments: false
      },

      node: {
        files: {
          "cassandra-vdba-driver.min.js": ["cassandra-vdba-driver.js"]
        }
      }
    }
  });

  //(2) enable plugins
  grunt.loadNpmTasks("grunt-contrib-clean");
  grunt.loadNpmTasks("grunt-contrib-compress");
  grunt.loadNpmTasks("grunt-contrib-concat");
  grunt.loadNpmTasks("grunt-contrib-jshint");
  grunt.loadNpmTasks("grunt-contrib-uglify");
  grunt.loadNpmTasks("grunt-jsdoc");
  grunt.loadNpmTasks("grunt-mocha-test");

  //(3) define tasks
  grunt.registerTask("testSpecific", "Specific unit testing", [
    "clean:test",
    "mochaTest:specific"
  ]);

  grunt.registerTask("api.html.zip", "Generates the API doc.", [
    "clean:doc",
    "jsdoc:api.html",
    "compress:api.html",
    "clean:doc",
  ]);

  grunt.registerTask("default", "All.", [
    "jshint:grunt",
    "jshint:test",
    "jshint:lib",
    //"concat:node",
    //"uglify:node",
    //"api.html.zip",
    "mochaTest"
  ]);
};

