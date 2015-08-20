
var uuid = require('node-uuid');
var async = require('async');
var amqp = require('amqp');

var dm = require('./index').datamover;

exports.job = function(conf) {
    this.id = uuid.v4(); //random
    this.name = conf.name || "job "+this.id;
    this.tasks = [];

    this.status = {name: this.name, tasks: {}};
}

exports.job.prototype.task = function(name, task) {
    var job = this;

    //attach extra attributes to task function
    task.id = String(job.tasks.length);
    task._name = name; 

    this.tasks.push(task);
    
    //report to progress service that this task exists!
    //TODO - let caller decode on the weight (or should we even use it?)
    job.progress({name: name, progress: 0, status: 'waiting', msg: 'Task registered', weight: 1}, job.id+'.'+task.id);
}

exports.job.prototype.progress = function(p, key) {
    if(key == undefined) key = this.id;
    if(dm.progress_ex) {
        //dm.logger.info(key);
        dm.progress_ex.publish(key, p);
    } else {
        dm.logger.debug(p); 
    }

    //update status 
    var node = this.getstatusnode(key);
    for (var at in p) { node[at] = p[at]; }
}

exports.job.prototype.getstatusnode = function(key) {
    //also store status information locally (redundant with progress service)
    var tokens = key.split(".");
    //first, identify the edge node to update
    tokens.shift(); //skip the jobid
    var node = this.status;
    tokens.forEach(function(token) {
        if(node.tasks[token] == undefined) {
            node.tasks[token] = {tasks: {}};
        }
        node = node.tasks[token];
    });
    return node;
}

exports.job.prototype.run = function(done) {
    var job = this;
    dm.logger.info("running job: "+job.id);
    async.eachSeries(this.tasks, function(task, cb) {
        job.progress({status: 'running', msg: 'Task starting'}, job.id+'.'+task.id);
        task(task, function(err, cont) {
            if(err) {
                dm.logger.error("task failed jobid:"+job.id+" taskid:"+task.id+ " taskname:"+task._name);
                dm.logger.error(JSON.stringify(err));
                if(cont) {
                    job.progress({status: 'failed', msg: (err.msg?err.msg:"Failed") + " :: continuing"}, job.id+'.'+task.id);
                    cb(); //return null to continue job
                } else {
                    job.progress({status: 'failed', msg: (err.msg?err.msg:"Failed") + " :: aborting job"}, job.id+'.'+task.id);
                    cb(err);
                }
            } else {
                //all good!
                //var node = job.getstatusnode(job.id+'.'+task.id);
                //dm.logger.info("task finished successfully: "+job.id+'.'+task.id);
                job.progress({progress: 1, status: 'finished', msg: 'Task finished Successfully'}, job.id+'.'+task.id);
                cb();
            }
        });
    }, function() {
        dm.logger.info("completed job: "+job.id);
        job.progress({status: 'finished'}, job.id);
        //console.log(JSON.stringify(job.status, null, 4));
        done(); //TODO should I pass job error / status etc back?
    });
}

