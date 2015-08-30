
//node
var assert = require('assert');

//contrib
var uuid = require('node-uuid');
var async = require('async');
var amqp = require('amqp');

//mine
var dm = require('./index').datamover;

exports.job = function(conf) {
    this.id = conf.id || uuid.v4(); //random
    this.name = conf.name || "Job:"+this.id;
    this.execution_mode = conf.type || 'serial';
    this.tasks = [];
    this.status = {tasks: []}; //for local status tree

    this.progress({name: this.name, progress: 0, status: 'waiting', msg: 'Job registered'});
}

exports.job.prototype.addTask = function(task) {
    //set default for missing data
    var job = this;
    if(task.id === undefined) task.id = String(this.tasks.length);
    if(task.weight === undefined) task.weight = 1;
    if(task.name === undefined) task.name = "Task:"+task.id+" for job:"+this.id;
    assert(task.work != undefined); //work must exist

    task.progress = function(p) {
        console.log(job.id+"."+task.id);
        job.progress(p, job.id+"."+task.id);
    }

    this.tasks.push(task);
    
    //report to progress service that this task exists
    this.progress({
        name: task.name, 
        progress: 0, 
        status: 'waiting', /*msg: 'Task created',*/ 
        weight: task.weight
    }, this.id+'.'+task.id);

    return task;
}

exports.job.prototype.addJob = function(conf) {
    //use the task id as job id
    conf.id = this.id+"."+String(this.tasks.length);
    console.log("adding job");
    console.dir(conf);

    var subjob = new exports.job(conf);
    var subtask = this.addTask({
        name: conf.name,
        work: function(task, cb) {
            subjob.run(cb);
        }
    });
    return subjob;
}

exports.job.prototype.progress = function(p, key) {
    if(key == undefined) key = this.id;
    if(dm.progress_ex) {
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
    job.progress({status: 'running', msg:'Processing tasks'}, job.id);

    function runtask(task, cb) {
        job.progress({status: 'running', msg: 'Running task'}, job.id+'.'+task.id);
        task.work(task, function(err, cont) {
            if(err) {
                dm.logger.error("task failed jobid:"+job.id+" taskid:"+task.id+ " taskname:"+task.name);
                dm.logger.error(JSON.stringify(err));
                if(cont) {
                    job.progress({status: 'failed', msg: (err.msg || JSON.stringify(err)) + " Continuing job"/*, progress: 1*/}, job.id+'.'+task.id);
                    cb(); //return null to continue job
                } else {
                    job.progress({status: 'failed', msg: (err.msg || JSON.stringify(err)) + " :: aborting job"}, job.id+'.'+task.id);
                    cb(err);
                }
            } else {
                //all good!
                //var node = job.getstatusnode(job.id+'.'+task.id);
                //dm.logger.info("task finished successfully: "+job.id+'.'+task.id);
                job.progress({progress: 1, status: 'finished', msg: 'Finished running task'}, job.id+'.'+task.id);
                cb();
            }
        });
    }

    function finishjob(err) {
        dm.logger.info("completed job: "+job.id);
        if(err) {
            job.progress({status: 'failed', msg: 'Job failed : '+err.toString()}, job.id);
        }  else {
            job.progress({status: 'finished', msg: 'All task completed'}, job.id);
        }
        //console.log(JSON.stringify(job.status, null, 4));
        done(); //TODO should I pass job error / status etc back?
    }

    if(job.execution_mode == 'serial') {
        async.eachSeries(this.tasks, runtask, finishjob);
    } else if(job.execution_mode == 'parallel') {
        //TODO untested
        async.eachParallel(this.tasks, runtask, finishjob);
    } else {
        throw new Exception("unknown execution mode:"+this.execution_mode);
    }
}

