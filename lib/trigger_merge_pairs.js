
var util  = require('util'),
    spawn = require('child_process').spawn;
var path = require('path');
var fs = require('fs');
var async = require('async');
var _ = require('lodash');
var get_files = require('./get_files')
var suss_detector_id = require('suss_detector_id')
var couch_check = require('couch_check_state')
var wim_sites = require('calvad_wim_sites')

var num_CPUs = require('os').cpus().length;

var statedb = 'vdsdata%2ftracking'

var R;
var years = [2010]//,2010,2011];
var RCall = ['--no-restore','--no-save','merge_wim_vds_pairs_distributed.R']

var trigger_R_job = function(task,done){
    var wim = task.wim
    var direction = task.direction

    task.env['RYEAR']=task.year
    task.env['WIM_SITE']=wim
    task.env['WIM_DIRECTION']=direction

    var R  = spawn('Rscript', RCall, task);
    R.stderr.setEncoding('utf8')
    R.stdout.setEncoding('utf8')
    var logfile = ['log/wimvdspair',wim,direction,task.year].join('_')+'.log'
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
var unique_wim = {}
var opts = { cwd: undefined,
             env: process.env
           }

var rootdir = path.normalize(__dirname)
var config_file = rootdir+'/config.json'
var config={}

_.each(years,function(year){
    wim_sites.get_wim_need_pairing({'year':year
                                   ,'config_file':config_file}
                                  ,function(e,r){
                                       console.log(r)
                                       _.each(r.rows,function(row){
                                           var w = row.key[1]
                                           var d = row.key[2]
                                           if(unique_wim[w+'.'+d+'.'+year] === undefined){
                                               var _opts = _.clone(opts)
                                               _opts.wim=w
                                               _opts.direction=d
                                               _opts.year=year

                                               file_queue.push(_opts
                                                              ,function(){
                                                                   console.log('wim site '+w+' '+d+' '+year+' done, '
                                                                              +file_queue.length()
                                                                              +' files remaining')
                                                                   return null
                                                               })

                                               unique_wim[w+'.'+d+'.'+year]=1
                                           }
                                           return null
                                       })
                                   })
});
