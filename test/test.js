
//var winston = require('winston');
var expect = require('chai').expect;
var assert = require('assert');
//var request = require('supertest');  
//var amqp = require('amqp');
//var uuid = require('node-uuid');
//var winston = require('winston');

//mine
var datamover = require('../index');

//mine
//var config = require('../api/config/config');
//var logger = new winston.Logger(/*config.logger.winston*/);
//var controllers = require('../api/controllers');
//var app = require('../api/server').app;

describe("job", function() {
    //var conn = null;
    //var ex = null;
    //var id = uuid.v4();

    before(function(done) {
        //logger.info("before here");
        console.log("before here");
        done();
    });

    describe("run job", function() {
        it("test", function(done) {
            //contstuct a test job
            var j = new datamover.job({name: "test job"});
            j.addTask({name: 'test task', work: function(task, cb) {
                j.progress({status: 'running', msg: 'Issuing timeout to simulate a job'});
                console.log("starting task");
                setTimeout(function() {
                    console.log("finished job");
                    cb();
                },  500);
            }});
            j.run(function() {
                console.log("job finished");
                done();
            });
        });
    });
});


