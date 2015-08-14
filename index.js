'use strict';

var uuid = require('node-uuid');
var async = require('async');
var spawn = require('child_process').spawn;

var logger = require('winston');

exports.init = function(conf) {
    if(conf.logger) logger = conf.logger;
}

exports.job = function(conf) {
    this.id = uuid.v4(); //random
    this.name = conf.name;
    this.tasks = [];
}

exports.job.prototype.task = function(name, task) {
    var job = this;

    task.id = uuid.v1(); //time
    task._name = name; //attach an attribute to task function
    this.tasks.push(task);
    
    //report to progress service that this task exists!
    //TODO - let caller decode on the weight (or should we even use it?)
    job.progress({progress: 0, status: 'waiting', msg: 'Staged', weight: 1}, job.id+'.'+task.id);
}

exports.job.prototype.progress = function(p, key) {
    if(key == undefined) key = job.id;
    console.log("progress(TODO) key:"+key);
    console.dir(p);
}

exports.job.prototype.run = function() {
    var job = this;
    async.eachSeries(this.tasks, function(task, cb) {
        job.progress({status: 'running', msg: 'Starting :: '+task._name}, job.id+'.'+task.id);
        
        task(task, function(err, cont) {
            if(err) {
                logger.error("task: failed jobid:"+job.id+" taskid:"+task.id);
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
        console.log("job completed");
        job.progress({progress: 1, status: 'finished'}, job.id);
    });
}

//common tasks goes here
exports.tasks = {
    tarfiles: function(conf, cb) {
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
        console.log("zipping "+conf.dest+" "+conf.path+ " at " +conf.cwd);
        //TODO - if you change the zip directory structure, make sure you are zipping all subdirectories and sub files, etc..
        var tar = spawn('zip', ['-r', conf.dest, conf.path], {cwd: conf.cwd});        
        tar.stderr.pipe(process.stderr);
        tar.on('close', function(code, signal) {
            if(code == 0) cb(null);
            else cb({code:code, signal:signal});
        });
    },
};


