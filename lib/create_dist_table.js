var extract_couch_docs = require('../lib/extract_couch_docs.js')
var pg = require('pg')

function setup_connection(opts){

    var host = opts.postgresql.host ? opts.postgresql.host : '127.0.0.1';
    var user = opts.postgresql.auth.username ? opts.postgresql.auth.username : 'myname';
    //var pass = opts.postgresql.auth.password ? opts.postgresql.password : '';
    var port = opts.postgresql.port ? opts.postgresql.port :  5432;
    var db  = opts.postgresql.grid_merge_sqlquery_db ? opts.postgresql.grid_merge_sqlquery_db : 'spatialvds'
    var connectionString = "pg://"+user+"@"+host+":"+port+"/"+db
    return connectionString
}

function create_distance_table(opts,cb){
    // use postgresql to get a distance table from all known WIM ids
    // to all known VDS ids that are good for this year

    // create teh big sql statement
    var sites = extract_couch_docs.extract_wim_sites(opts.wim_sites)
    var vdsids = extract_couch_docs.extract_vds_ids(opts.vds_sites)

    var latest = 'latest as (select id,max(version) as version from vds_versioned group by id order by id)'
    var vds_geoview =
            'vds_geoview as ('+
            'SELECT v.id,' +
            'v.name,' +
            'v.cal_pm,' +
            'v.abs_pm,' +
            'v.latitude,' +
            'v.longitude,' +
            'vv.lanes,' +
            'vv.segment_length,' +
            'vv.version,' +
            'vf.freeway_id,' +
            'vf.freeway_dir,' +
            'vt.type_id AS vdstype,' +
            'vd.district_id AS district,' +
            'g.gid,' +
            'g.geom' +
            ' FROM vds_id_all v' +
            ' JOIN vds_points_4326 ON v.id = vds_points_4326.vds_id' +
            ' JOIN latest USING(id)' +
            ' JOIN vds_versioned vv ON (vv.id=v.id and vv.version=latest.version)'+
            ' JOIN vds_vdstype vt USING (vds_id)' +
            ' JOIN vds_district vd USING (vds_id)' +
            ' JOIN vds_freeway vf USING (vds_id)' +
            ' JOIN geom_points_4326 g USING (gid)'+
            ' WHERE v.id in ('+
            vdsids.join(',')+
            '))'

    var wim_sites =
            'wim_sites as (select * from wim_geoview w where site_no < 800 and '+
            'site_no in (' +
            sites.join(',') +
            '))'

    var vds_wim_dist =
            'vds_wim_dist as ('+
            ' select distinct v.id as vds_id,w.site_no,'+
            ' v.freeway_id as freeway,'+
            ' newctmlmap.canonical_direction(w.direction) as direction,'+
            ' ST_Distance_Sphere(v.geom,w.geom) as dist'+
            ' from vds_geoview v'+
            ' left outer join wim_sites w on  ('+
            '            w.freeway_id=v.freeway_id and'+
            '            newctmlmap.canonical_direction(w.direction)='+
            '              newctmlmap.canonical_direction(v.freeway_dir)'+
            '             ))'

    var with_statement =
            'with '+
            latest+', '+
            vds_geoview+', '+
            wim_sites+', '+
            vds_wim_dist

    var select_statement =
            'SELECT * ' + //'INTO TEMP vds_wim_distance'+
            ' FROM vds_wim_dist'+
            ' WHERE site_no is not null'+
            ' ORDER BY site_no,direction,dist,vds_id;'


    var query = with_statement + ' ' + select_statement


    var connectionString = setup_connection(opts)

    console.log(query)
    pg.connect(connectionString
               ,function(err,client,clientdone){
                   if(err) return cb(err)
                   client.query(query,function(e,r){
                       if(e){

                           console.log('query failed',query)
                           clientdone()
                           return cb(e)

                       }
                       // have the distance table as a list of records
                       cb(null,r)
                       clientdone()
                       return null
                   })
                   return null
               })
    return null
}

module.exports=create_distance_table
