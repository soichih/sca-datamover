
//node
var assert = require('assert');

//contrib
var uuid = require('uuid');
var async = require('async');
var amqp = require('amqp');

//mine
var dm = require('./index').datamover;

//set job.request_stop = true to abort tasks
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

    //inherit the same status object from parent
    conf.status = this.status;

    var subjob = new exports.job(conf);
    var subtask = this.addTask({
        name: conf.name,
        work: function(task, cb) {
            subjob.run(cb);
        },
        job: subjob
    });
    return subjob;
}

exports.job.prototype.progress = function(p, key) {
    if(key == undefined) key = this.id;
    if(dm.progress_ex) {
        dm.progress_ex.publish("_isdp."+key, p);
    } //else {
    dm.logger.debug(["_isdp."+key, p]); 

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

exports.job.prototype.stop = function(done) {
    this.request_stop = true;
    this.tasks.forEach(function(task) {
        if(task.job) task.job.stop();
    }); 
}

exports.job.prototype.run = function(done) {
    var job = this;
    dm.logger.info("running job: "+job.id);
    job.progress({status: 'running', msg:'Processing tasks'}, job.id);

    function runtask(task, cb) {
        if(job.request_stop) {
            job.progress({status: 'canceled', msg: 'Stop requested'}, job.id+'.'+task.id);
            return cb();
        }
        job.progress({status: 'running', msg: 'Running task'}, job.id+'.'+task.id);
        task.work(task, function(err, cont) {
            if(err) {
                dm.logger.error("task failed jobid:"+job.id+" taskid:"+task.id+ " taskname:"+task.name);
                dm.logger.error(JSON.stringify(err));
                if(cont) {
                    job.progress({status: 'failed', msg: (err.msg || JSON.stringify(err)) + " :: continuing job"/*, progress: 1*/}, job.id+'.'+task.id);
                    cb(); //return null to continue job by ignoring the error
                } else {
                    job.progress({status: 'failed', msg: (err.msg || JSON.stringify(err)) + " :: aborting job"}, job.id+'.'+task.id);
                    cb(err);
                }
            } else {
                //all good!
                var p = {status: 'finished', msg: 'Finished running task'};
                if(!task.job) p.progress = 1; //edge task.. let's report 100% completion.
                var p = job.progress(p, job.id+'.'+task.id);
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
        done(err);
    }

    if(job.execution_mode == 'serial') {
        async.eachSeries(this.tasks, runtask, finishjob);
    } else if(job.execution_mode == 'parallel') {
        async.eachParallel(this.tasks, runtask, finishjob); //TODO untested
    } else {
        throw new Exception("unknown execution mode:"+this.execution_mode);
    }
}

