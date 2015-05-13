// this will pair up VDS and WIM sites, then merge the imputed output.
// while I still haven't yet done the command line thing, the idea is
// to process data for a year *after* finishing self-imputing step.


var util  = require('util'),
    spawn = require('child_process').spawn;
var path = require('path');
var fs = require('fs');
var queue = require('queue-async');
var _ = require('lodash');
var couch_check = require('couch_check_state')

var vdspath = process.env.VDS_PATH
if(!vdspath){
    throw new Error('assign a value to env variable VDS_PATH')
}
var wimpath = process.env.WIM_PATH
if(!wimpath){
    throw new Error('assign a value to env variable WIM_PATH')
}

var num_CPUs = require('os').cpus().length;

num_CPUs=1 // while testing

var R;


var years = [2012];




var unique_wim = {}

var rootdir = path.normalize(__dirname)

var RCall = ['--no-restore','--no-save','merge_wim_vds_pairs.R']
var config_file = path.normalize(rootdir+'/../config.json')
var Rhome = rootdir+'/../R'


var config_okay=require('config_okay')

var vds_imputed = require('../lib/vds_imputed.js')
var wim_imputed = require('../lib/wim_imputed.js')
var create_dist_table = require('../lib/create_dist_table.js')



var pair_wim_vds = function(wim_site,vds_ids,Ropt,done){

    var site_dir = wim_site.split('.').slice(1)
    var Renv = _.extend({},Ropt.env)

    Renv.env['RYEAR']=Ropt.year
    Renv.env['WIM_SITE']=site_dir[0]
    Renv.env['WIM_DIRECTION']=site_dir[1]
    Renv.env['VDS_IDS'] = vds_ids
    Renv.env['R_CONFIG'] = config_file

    var R  = spawn('Rscript', RCall, Renv);
    R.stderr.setEncoding('utf8')
    R.stdout.setEncoding('utf8')
    var logfile = 'log/wimvds_pair_'+
            site_dir[0]+'_'+
            site_dir[1]+'_'+
            Ropt.year+'.log'
    var logstream = fs.createWriteStream(logfile
                                        ,{flags: 'a'
                                         ,encoding: 'utf8'
                                         ,mode: 0666 })
    R.stdout.pipe(logstream)
    R.stderr.pipe(logstream)
    R.on('exit',function(code){
        console.log('got exit: '+code+', for ',wim_site)
        // testing
        throw new Error('croak')
        return done()
    })
}

var yearQ = queue()
var processQ = queue(num_CPUs)

function process_years(config){

    yearQ = queue()
    years.forEach(function(year){
        yearQ.defer(function(cb){

            var Ropt =_.extend({'year':year
                                ,'cwd': Rhome
                                ,'env': process.env
                               },config)
            var couch_opt =_.extend({'year':year
                                    },config.couchdb)

            // get this year's valid WIM and VDS, and then set up the temp
            // table to look up distances using only those two sets of detectors.
            // valid means couchdb says imputation finished.
            // so I need a view that says as such
            var seQ = queue()
            seQ.defer(vds_imputed, couch_opt)
            seQ.defer(wim_imputed, couch_opt)

            seQ.await(function(e,vds_sites,wim_sites){
                Ropt.vds_sites = vds_sites
                Ropt.wim_sites = wim_sites
                create_dist_table(Ropt,function(e,r){
                    // okay distance table is created in the current psql db conn
                    Object.keys(r).forEach(function(wim_site){
                        // there is at least one site with distance 25,000+
                        // that was used as a pair in 2009
                        // otherwise, skip the wim sites with distance > 26000
                        if(wim_site,r[wim_site][0].dist < 26000){
                            processQ.defer(pair_wim_vds,
                                           wim_site,
                                           r[wim_site].slice(0,5),
                                           Ropt)
                        }

                        return null
                    })
                    return cb() // done with year's deferred handler,
                                // wimQ loaded for this year

                })
                return null
            })
            return null

        })
        return null


    })
    yearQ.await(function(e,r){
        if(e) throw new Error(e)
        processQ.await(function(ee,rr){
            console.log('wim queue drained for all years')
            return null
        })
    })
}

config_okay(config_file,function(err,c){
    if(err){
        throw new Error('node.js needs a good croak module')
    }
    process_years(c)
    return null
})
