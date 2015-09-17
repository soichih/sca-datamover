
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
    this.status = conf.status || {tasks: [], id: this.id}; //for local status tree

    this.tasks = [];

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
        //console.log(job.id+"."+task.id);
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
    //dm.logger.debug("adding subjob: "+conf.id); 

    //inherit the same status object from parent
    conf.status = this.status;

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

    //update local status 
    var node = this.getstatusnode(key);
    for (var at in p) { node[at] = p[at]; }
}

//locate local status (incase user don't want to use progress service) node from given key
exports.job.prototype.getstatusnode = function(key) {
    var tokens = key.split(".");
    var node = this.status; //set to root
    tokens.shift(); 
    tokens.forEach(function(token) {
        var subnode = null;
        node.tasks.forEach(function(task) {
            if(task.id == token) subnode = task;
        });
        //add if not found
        if(subnode == null) {
            //dm.logger.debug("adding new subnode on tasks in "+node.id);
            subnode = {tasks: [], id: token};
            node.tasks.push(subnode);
        }
        node = subnode; //move up
    });
    //console.log(JSON.stringify(this.status, null, 4));
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
                    cb(); //return null to continue job by ignoring the error
                } else {
                    job.progress({status: 'failed', msg: (err.msg || JSON.stringify(err)) + " :: aborting job"}, job.id+'.'+task.id);
                    cb(err);
                }
            } else {
                //all good!
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

