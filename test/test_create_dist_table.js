/* global require console process describe it */

var should = require('should')
var create_dist_table = require('../lib/create_dist_table.js')
var path    = require('path')
var rootdir = path.normalize(__dirname)


var config_file = rootdir+'/../test.config.json'
var config={}
var config_okay = require('config_okay')
var wim_imputed = require('../lib/wim_imputed.js')
var vds_imputed = require('../lib/vds_imputed.js')
var extract_couch_docs = require('../lib/extract_couch_docs.js')
var queue = require('queue-async')
var _ = require('lodash')

before(function(done){

    config_okay(config_file,function(err,c){
        if(err){
            throw new Error('node.js needs a good croak module')
        }
        config = c
        console.log(c.couchdb)
        var q = queue()
        var opts =_.extend({'year':2012},c.couchdb)
        q.defer(wim_imputed,opts)
        q.defer(vds_imputed,opts)
        q.await(function(e,w,v){
            config.wim_sites = w
            config.vds_sites = v

            return done()

        })
        return null
    })
    return null
})

describe('query wim vds distances',function(){
    it('should get distance table for wim vds',function(done){
        create_dist_table(config,function(e,r){
            console.log(r.slice(0,10))
            return done()

        })
    })

})
