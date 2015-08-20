
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
    },

    zipfiles: function(conf, cb) {
        //logger.info("zipping "+conf.dest+" "+conf.path+ " at " +conf.cwd);
        //TODO - if you change the zip directory structure, make sure you are zipping all subdirectories and sub files, etc..
        var zip = spawn('zip', ['-r', conf.dest, conf.path], {cwd: conf.cwd});        
        //zip.stderr.pipe(dm.logger);
        //zip.stdout.pipe(concat(logout));
        zip.stderr.pipe(concat(logerror));
        zip.on('close', function(code, signal) {
            if(code == 0) cb(null);
            else cb({code:code, signal:signal});
        });
        zip.stdout.on('data', function(data) {
            if(conf.on_progress) conf.on_progress(data.toString());
        });
    },
};


