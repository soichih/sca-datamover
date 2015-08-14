'use strict';

var uuid = require('node-uuid');
var async = require('async');
var spawn = require('child_process').spawn;

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
        job.progress({status: 'running', msg: 'Starting '+task._name}, job.id+'.'+task.id);
        
        task(task, function(err) {
            if(err) {
                console.log("task failed");
                console.log(JSON.stringify(err));
                job.progress({status: 'failed', msg: "Failed"}, job.id+'.'+task.id);
            } else {
                job.progress({progress: 1, status: 'finished'}, job.id+'.'+task.id);
            }
            cb(err);
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
        tar.on('close', function(code, signal) {
            if(code == 0) cb(null);
            else cb({code:code, signal:signal});
        });
    },
};


