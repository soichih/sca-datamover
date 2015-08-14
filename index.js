'use strict';

var uuid = require('node-uuid');
var async = require('async');
var spawn = require('child_process').spawn;
var logger = require('winston'); //will be replaced by init
var amqp = require('amqp');

var progress_ex = null;
exports.init = function(conf, cb) {
    if(conf.logger) logger = conf.logger;
    if(conf.progress) {
        var conn = amqp.createConnection(conf.progress.amqp, {reconnectBackoffTime: 1000*10});
        conn.on('ready', function() {
            logger.info("amqp handshake complete");
            progress_ex = conn.exchange(conf.progress.exchange, {autoDelete: false, durable: true, type: 'topic'}, function(ex) {
                logger.info("amqp connected to exchange:"+conf.progress.exchange);
            });
        });
        conn.on('error', function(err) {
            logger.warn("amqp received error.", err);
        });
    }
}

exports.job = function(conf) {
    this.id = uuid.v4(); //random
    this.name = conf.name;
    this.tasks = [];
}

exports.job.prototype.task = function(name, task) {
    var job = this;

    task.id = String(job.tasks.length);
    task._name = name; //attach an attribute to task function
    this.tasks.push(task);
    
    //report to progress service that this task exists!
    //TODO - let caller decode on the weight (or should we even use it?)
    job.progress({progress: 0, status: 'waiting', msg: 'Staged', weight: 1}, job.id+'.'+task.id);
}

exports.job.prototype.progress = function(p, key) {
    if(key == undefined) key = job.id;
    if(progress_ex) {
        //logger.info(key);
        progress_ex.publish(key, p);
    } else {
        logger.debug(p); 
    }
}

exports.job.prototype.run = function() {
    var job = this;
    logger.info("running job: "+job.id);
    async.eachSeries(this.tasks, function(task, cb) {
        logger.info(task._name+ " :: job:"+job.id);
        job.progress({status: 'running', msg: 'Starting :: '+task._name}, job.id+'.'+task.id);
        task(task, function(err, cont) {
            if(err) {
                logger.error("task failed jobid:"+job.id+" taskid:"+task.id);
                logger.error(err);
                if(cont) {
                    job.progress({status: 'failed', msg: "Failed - skipping this task"}, job.id+'.'+task.id);
                    cb();
                } else {
                    job.progress({status: 'failed', msg: "Failed - aborting job"}, job.id+'.'+task.id);
                    cb(err);
                }
            } else {
                //all good!
                job.progress({progress: 1, status: 'finished'}, job.id+'.'+task.id);
                cb();
            }
        });
    }, function() {
        logger.info("completed job: "+job.id);
        job.progress({progress: 1, status: 'finished'}, job.id);
    });
}

//common tasks goes here
exports.tasks = {
    tarfiles: function(conf, cb) {
        //logger.info("tarring "+conf.dest+" "+conf.path+ " at " +conf.cwd);
        var cmd = '-cf';
        if(conf.gzip) cmd = '-czf'; 
        var tar = spawn('tar', [cmd, conf.dest, conf.path], {cwd: conf.cwd});        
        tar.stderr.pipe(process.stderr);
        tar.on('close', function(code, signal) {
            if(code == 0) cb(null);
            else cb({code:code, signal:signal});
        });
    },

    zipfiles: function(conf, cb) {
        //logger.info("zipping "+conf.dest+" "+conf.path+ " at " +conf.cwd);
        //TODO - if you change the zip directory structure, make sure you are zipping all subdirectories and sub files, etc..
        var tar = spawn('zip', ['-r', conf.dest, conf.path], {cwd: conf.cwd});        
        tar.stderr.pipe(process.stderr);
        tar.on('close', function(code, signal) {
            if(code == 0) cb(null);
            else cb({code:code, signal:signal});
        });
    },
};


