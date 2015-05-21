/* global require console process describe it */

var should = require('should')
var vds_imputed = require('../lib/vds_imputed.js')
var path    = require('path')
var rootdir = path.normalize(__dirname)
var _ = require('lodash')
var config_okay=require('config_okay')


var config_file = rootdir+'/../test.config.json'
var config={}

before(function(done){

    config_okay(config_file,function(e,c){

        config = c
        return done()
    })
})
describe('get finished imputed VDS sites',function(){
    it('should get the VDS sites using passed in couch opts',function(done){
        var vds_imputed2 = require('../lib/vds_imputed.js')
        var opt = _.extend({
            'year':2012
        },config)
        vds_imputed2(opt,function(e,sites){
                        should.not.exist(e)
                        sites.should.have.lengthOf(493)
                        return done()
                    })
    })
    it('should get zero VDS sites from 2011, as nothing is done there yet',function(done){
        var opt = _.extend({
            'year':2011
        },config)
        vds_imputed(opt
		    ,function(e,sites){
                        should.not.exist(e)
                        sites.should.have.lengthOf(0)
                        return done()
                    })
    })

})
