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

    print(paste("dim(df.vds.merged) <- (", paste(dim(df.vds.merged),collapse=','),')'))

    df.merged <- merge(df.vds.merged, df.wim.merged,
                       all=FALSE,
                       suffixes = c("vds","wim"))

    ## entering all=FALSE above truncates the non-overlap

    df.merged
}


##' evaluate paired data
##'
##' This function fixed an even uglier hack done earlier for speed.
##' The logic is to sift through the names, keep what I need, discard
##' (?) what I don't wim data, needs to have wim.lanes worth of info
##'
##' @title evaluate.paired.data
##' @param df the data frame with paired data
##' @param wim.lanes lanes at the WIM site
##' @param vds.lanes lanes at the VDS site
##' @return a dataframe that equals
##' df[,c(vds.vars.lanes,wim.vars.lanes,other.vars)] where
##' vds.vars.lanes is the vds variables (vol, occ), wim.vars.lanes is
##' the wim variabes (*hh, *weight,*axle, and *speed variables, see
##' the code for the exact), and other.vars are other variables
##' @author James E. Marca
evaluate.paired.data <- function(df,wim.lanes=0,vds.lanes){
    paired.data.names <- names(df)

    if(wim.lanes == 0){
        print('guessing wim lanes')
        wim.lanes <- calvadrscripts::longway.guess.lanes(df)
        print(wim.lanes)
    }
    wim.var.pattern <-
        "(heavyheavy|_weight|_axle|_len|_speed)"
    ## "(heavyheavy|_weight|_axle|_len|_speed|_all_veh_speed)"

    wim.vars <- grep(pattern=wim.var.pattern,x=paired.data.names
                     ,perl=TRUE,value=TRUE)
    other.vars <- grep(pattern=wim.var.pattern,x=paired.data.names
                       ,perl=TRUE,value=TRUE,invert=TRUE)
    lanes.vars <- c()
    for(lane in 1:wim.lanes){
        lane.pattern <- paste("r",lane,sep='')
        lane.vars <- grep(pattern=lane.pattern,x=wim.vars
                          ,perl=TRUE,value=TRUE)
        lanes.vars <- c(lanes.vars,lane.vars)
    }
    wim.vars.lanes <- lanes.vars


    vds.var.pattern <- "(^nl|^nr\\d|^ol|^or\\d)"
    vds.vars <- grep(pattern=vds.var.pattern,x=paired.data.names
                     ,perl=TRUE,value=TRUE)
    other.vars <- grep(pattern=vds.var.pattern,x=other.vars
                       ,perl=TRUE,value=TRUE,invert=TRUE)

    ## expect_that(sort(c(other.vars
    ##                    ,vds.vars,wim.vars))
    ##             ,equals(sort(paired.data.names)))
    ## passed in testing

    ## reset
    lanes.vars <- c()

    ## need to process lanes right and left lane separately right lane
    ## is numbered from the right as r1 to r(n-1).  The left lane is
    ## always numbered l1.  If a site has one lane, that lane, by
    ## definition, is r1.  If a site has two lanes, the lanes are r1
    ## and l1, again, by definition.  If a site has three lanes, (n=3)
    ## then the left lane is l1, and the other lanes are numbered r1
    ## and r2, AKA r(n-1)

    ## so special case is n=1 (right lane only)
    right.lanes <- vds.lanes
    if(vds.lanes>1){
        ## there *is* a left lane for all cases when n>1
        right.lanes <- vds.lanes-1
        lane.pattern <- "l1"
        lane.vars <- grep(pattern=lane.pattern,x=vds.vars
                          ,perl=TRUE,value=TRUE)
        lanes.vars <- lane.vars
    }
    for(lane in 1:right.lanes){
        ## in the case when vds.lanes==1, right.lanes also == 1
        lane.pattern <- paste("r",lane,sep='')
        lane.vars <- grep(pattern=lane.pattern,x=vds.vars
                          ,perl=TRUE,value=TRUE)
        lanes.vars <- c(lanes.vars,lane.vars)
    }
    vds.vars.lanes <- lanes.vars

    pared.df <- df[,c(vds.vars.lanes,wim.vars.lanes,other.vars)]
    pared.df
}
