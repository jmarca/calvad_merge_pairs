
var util  = require('util'),
    spawn = require('child_process').spawn;
var path = require('path');
var fs = require('fs');
var async = require('async');
var _ = require('lodash');
var get_files = require('./get_files')
var suss_detector_id = require('suss_detector_id')
var couch_check = require('couch_check_state')

var num_CPUs = require('os').cpus().length;

var statedb = 'vdsdata%2ftracking'

var R;
var trigger_R_job = function(task,done){
    var wim = task.wim

    task.env['RYEAR']=task.year
     task.env['WIM_SITE']=wim
    task.env['WIM_IMPUTE']=1
    task.env['WIM_PLOT']=1

    var R  = spawn('Rscript', RCall, task);
    R.stderr.setEncoding('utf8')
    R.stdout.setEncoding('utf8')
    var logfile = 'log/wimimpute_'+wim+'_'+task.year+'.log'
    var logstream = fs.createWriteStream(logfile
                                        ,{flags: 'a'
                                         ,encoding: 'utf8'
                                         ,mode: 0666 })
    R.stdout.pipe(logstream)
    R.stderr.pipe(logstream)
    R.on('exit',function(code){
        console.log('got exit: '+code+', for ',wim)
        // throw new Error('die')
        return done()
    })
}
var file_queue=async.queue(trigger_R_job,num_CPUs)
file_queue.drain =function(){
    console.log('queue drained')
    return null
}
