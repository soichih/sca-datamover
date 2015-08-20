'use strict';

var amqp = require('amqp');

//datamover global stuff
var dm = {
    logger: require('winston'),
    progress_ex: null
};
exports.datamover = dm;

//initialize stuff that datamover uses
exports.init = function(conf, cb) {
    if(conf.logger) dm.logger = conf.logger;
    if(conf.progress) {
        var conn = amqp.createConnection(conf.progress.amqp, {reconnectBackoffTime: 1000*10});
        conn.on('ready', function() {
            dm.logger.info("amqp handshake complete");
            dm.progress_ex = conn.exchange(conf.progress.exchange, {autoDelete: false, durable: true, type: 'topic'}, function(ex) {
                dm.logger.info("amqp connected to exchange:"+conf.progress.exchange);
            });
        });
        conn.on('error', function(err) {
            dm.logger.warn("amqp received error.", err);
        });
    }
}

//job runner (not sure if this needs to be part of sca-mover..)
exports.job = require('./job').job;

//common tasks that user runs on job runner
exports.tasks = require('./tasks').tasks;


