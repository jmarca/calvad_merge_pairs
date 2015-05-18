##' Make a filename and the right parent directories for a merged file
##' of WIM and VDS data
##'
##' @title make.merged.filepath
##' @param vdsid the VDS id
##' @param year the year
##' @param wim.site the WIM site number
##' @param direction the direction of flow at the WIM site
##' @param rootpath the root directory for saving files
##' @return a vector with two values.  The first value is the path
##' where the merged pair data should be written, the second value is
##' the appropriate filename.
##' @author James E. Marca
##' @export
make.merged.filepath <- function(vdsid,year,wim.site,direction,rootpath){
    savepath <- paste(rootpath,year,sep='/')
    if(!file.exists(savepath)){dir.create(savepath)}
    savepath <- paste(savepath,wim.site,sep='/')
    if(!file.exists(savepath)){dir.create(savepath)}
    savepath <- paste(savepath,direction,sep='/')
    if(!file.exists(savepath)){dir.create(savepath)}
    filename <- paste('wim',wim.site,direction,
                      'vdsid',vdsid,
                      year,
                      'paired',
                      'RData',
                      sep='.')
    filepath <- paste(savepath,filename,sep='/')
    return (c(filepath,filename))
}

##' Check if there is an attachement already in the couchdb database
##' for this combo of WIM site, VDS id, and year
##'
##' @title couch.has.merged.pair
##' @param trackingdb the couchdb database name
##' @param vds.id the VDS id
##' @param wim.site the WIM site number
##' @param direction the direction of flow at the WIM site
##' @param year the year
##' @return TRUE if the database has an attachment, FALSE if not.
##' This does not actually retrieve the attachment, so it could be
##' junk.  This just checks if there *is* an attachment that is named
##' what it is supposed to be named to be a valid merged dataframe
##' @author James E. Marca
##' @export
couch.has.merged.pair <- function(trackingdb,vds.id,wim.site,direction,year){
    filepath <- make.merged.filepath(vds.id,year,wim.site,direction,'.')
    have_one <- rcouchutils::couch.has.attachment(db=trackingdb
                                                 ,docname=vds.id
                                                 ,attachment=filepath[2])
    return (have_one)
}
##' Get the merged pair dataframe from couchdb
##'
##' Will get the file that was previously attached to couchdb, then
##' will load it into R and return the dataframe
##' @title couch.get.merged.pair
##' @param trackingdb the tracking db couchdb
##' @param vds.id the vds.id....also the document id for couchdb
##' @param wim.site the wim site number
##' @param direction the direction of flow at the WIM site
##' @param year the year of the data
##' @return a dataframe containing whatever was attached to couchdb
##' @author James E. Marca
##' @export
couch.get.merged.pair <- function(trackingdb,vds.id,wim.site,direction,year){
    filepath <- make.merged.filepath(vds.id,year,wim.site,direction,'.')
    res <- rcouchutils::couch.get.attachment(db=trackingdb
                                            ,docname=vds.id
                                            ,attachment=filepath[2])
    varnames <- names(res)
    barfl <- res[[1]][[varnames[1]]]
    return(barfl)
}

##' put the merged data as a couchdb attachment.  The dataframe *must*
##' be saved to the filesystem first.
##'
##' @title couch.put.merged.pair
##' @param trackingdb what database to use
##' @param vds.id the VDS id...the document id in the database to use
##' @param file the complete filepath to the merged data.  Using the
##' first value from the output of make.merged.filepath
##' @return the output of the couch put function
##' @author James E. Marca
##' @export
couch.put.merged.pair <- function(trackingdb,vds.id,file){
    rcouchutils::couch.attach(db=trackingdb
                             ,docname=vds.id
                             ,attfile=file)
}

##' Merge a WIM imputed output dataframe with a VDS imputed output
##'
##' This function is pretty much just a wrapper around merge.  It is
##' passed a valid WIM imputation output that has been aggregated and
##' all that, as well as the vds.id, year, and file path for the VDS
##' data you want to pair with this WIM site.
##'
##' What this will do is simply merge by timestamp every entry in the
##' WIM data recrods with the corresponding timestamp entry in the VDS
##' data.
##'
##' The data are not merged using any sort of imputation code.
##'
##' @title merge_wim_with_vds
##' @param df.wim.merged A valid WIM dataframe containing the output
##' of the WIM self-imputation process, with just one entry per
##' timestamp
##' @param wim.site The WIM site number
##' @param direction The directon of flow at the WIM site
##' @param vds.id The VDS id for the site's imputation output you want
##' to pair with the WIM site
##' @param year The year
##' @param path the path in the local filesystem to grab the VDS
##' imputation output
##' @param trackingdb The couchdb tracking db.  Any issues will be
##' noted here
##' @return A dataframe containing the merged WIM and VDS data.  There
##' will not be any overlap.  That is, if there is no record in either
##' WIM or VDS for a particular timestamp, then the returned result
##' will not contain that time stamp.  In other words, if the VDS data
##' coveres June to August, and the WIM data covers January to May,
##' then the returned dataframe will be empty!  If the WIM covers the
##' whole year and the VDS only June to August, then the result will
##' just cover June to August.
##' @author James E. Marca
##' @export
merge_wim_with_vds <- function(df.wim.merged,
                               wim.site,
                               direction,
                               vds.id,
                               year,
                               path,
                               trackingdb){


    print(paste('loading',vds.id,'from',path))
    df.vds.merged <- calvadrscripts::get.and.plot.vds.amelia(
        pair=vds.id,
        year=year,
        doplots=FALSE,
        remote=FALSE,
        path=path,
        force.plot=FALSE,
        trackingdb=trackingdb)

    df.merged <- merge(df.vds.merged, df.wim.merged,
                       all=FALSE,
                       suffixes = c("vds","wim"))

    ## entering all=FALSE above truncates the non-overlap

    df.merged
}
