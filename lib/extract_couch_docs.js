
function extract_vds_ids(couch_docs){
    var vds_ids = couch_docs.map(function(v,k){
        return +v.id
    })
    return vds_ids
}

function extract_wim_sites(couch_docs){
    var wim_sites = couch_docs.map(function(w,k){
        var site_dir = w.id.split('.').slice(1)
        return +site_dir[0]
    })
    return wim_sites
}

function extract_wim_site_dirs(couch_docs){
    var wim_sites = couch_docs.map(function(w,k){
        var site_dir = w.id.split('.').slice(1)
        site_dir[0]= +site_dir[0]
        return site_dir
    })
    return wim_sites
}

module.exports.extract_vds_ids = extract_vds_ids
module.exports.extract_wim_sites = extract_wim_sites
module.exports.extract_wim_site_dirs = extract_wim_site_dirs
