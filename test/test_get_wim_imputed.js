/* global require console process describe it */

var should = require('should')
var wim_imputed = require('../lib/wim_imputed.js')
var path    = require('path')
var rootdir = path.normalize(__dirname)


var config_file = rootdir+'/../test.config.json'
var config={}
var config_okay = require('config_okay')
before(function(done){

    config_okay(config_file,function(err,c){
        if(err){
            throw new Error('node.js needs a good croak module')
        }
        config = c
        return done()
    })
    return null
})

describe('get finished imputed WIM sites',function(){
    it('should get the WIM sites that are finished imputed',function(done){
        config.year =2012
        wim_imputed(config
		    ,function(e,sites){
                        should.not.exist(e)
                        should.exist(sites)
                        sites.should.have.lengthOf(140)
                        return done()
                    })
    })

})
