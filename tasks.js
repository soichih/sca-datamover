
//os
var spawn = require('child_process').spawn;

//contrib
var concat = require('concat-stream');

//mystuff
var dm = require('./index').datamover;

function logerror(buffer) {
    dm.logger.error(buffer.toString());
}
function logout(buffer) {
    dm.logger.info(buffer.toString());
}

//common tasks
exports.tasks = {
    tarfiles: function(conf, cb) {
        //logger.info("tarring "+conf.dest+" "+conf.path+ " at " +conf.cwd);
        var cmd = '-cf';
        if(conf.gzip) cmd = '-czf'; 
        var tar = spawn('tar', [cmd, conf.dest, conf.path], {cwd: conf.cwd});        
        //tar.stderr.pipe(dm.logger);
        tar.stderr.pipe(concat(logerror));
        tar.stdout.pipe(concat(logout));
        tar.on('close', function(code, signal) {
            if(code == 0) cb(null);
            else cb({code:code, signal:signal});
        });
        tar.on('error', function(err) {
            dm.logger.error("tarfiles task failed using cwd:"+conf.cwd);
            dm.logger.error(err);
            //'close' will still fire so no need for cb(err);
        });
    },

    zipfiles: function(conf, cb) {
        //logger.info("zipping "+conf.dest+" "+conf.path+ " at " +conf.cwd);
        //TODO - if you change the zip directory structure, make sure you are zipping all subdirectories and sub files, etc..
        var zip = spawn('zip', ['-rm', conf.dest, conf.path], {cwd: conf.cwd});        
        //zip.stderr.pipe(dm.logger);
        //zip.stdout.pipe(concat(logout));
        zip.stderr.pipe(concat(logerror));
        zip.on('close', function(code, signal) {
            if(code == 0) cb(null);
            else cb({code:code, signal:signal});
        });
        zip.stdout.on('data', function(data) {
            //TODO parse output so that I can report a better progress report
            if(conf.on_progress) conf.on_progress(data.toString());
        });
        zip.on('error', function(err) {
            dm.logger.error("zipfiles task failed using cwd:"+conf.cwd);
            dm.logger.error(err);
            //'close' will still fire so no need for cb(err);
        });
    },
};


