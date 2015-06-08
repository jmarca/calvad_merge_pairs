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
##' @param vds.names names of vds dataframe, or minimal set of names
##'     that will generate accurate list of actual lanes at the VDS
##'     site
##'
##' @return a dataframe that equals
##'     df[,c(vds.vars.lanes,wim.vars.lanes,other.vars)] where
##'     vds.vars.lanes is the vds variables (vol, occ), wim.vars.lanes
##'     is the wim variabes (*hh, *weight,*axle, and *speed variables,
##'     see the code for the exact), and other.vars are other
##'     variables
##'
##' @author James E. Marca
##'
evaluate.paired.data <- function(df,vds.names){
    lanes_paired <- calvadrscripts::extract_unique_lanes(df)
    lanes_vds <- calvadrscripts::extract_unique_lanes(vds.names)
    if(is.null(lanes_vds)){
        stop(paste('no vds lanes detected in passed vds.names:',vds.names))
    }

    ## for later, extract the WIM lanes...those lanes with Truck data
    varnames <-  names(df)
    nhh_pattern <- 'not_heavyheavy'
    df_laned_vars <- grep(pattern=nhh_pattern,x=varnames,
                          perl=TRUE,value=TRUE,invert=FALSE)
    wim_unique_lanes <- calvadrscripts::extract_unique_lanes(df)


    ## now make those the same as best as I can by dropping from df.
    ## I can't *add* to the paired data frame though, so it is a
    ## one-way fixing operation

    laned_pattern <- paste(lanes_paired,'$',sep='',collapse='|')

    unlaned_vars <- grep(pattern=laned_pattern,x=names(df),
                         perl=TRUE,value=TRUE,invert=TRUE)

    vds_laned_pattern <- paste(lanes_vds,'$',sep='',collapse='|')
    laned_vars <- grep(pattern=vds_laned_pattern,x=names(df),
                       perl=TRUE,value=TRUE)
    keep_names <- c(laned_vars,unlaned_vars)

    trimmed_df <- df[,keep_names]
    names(trimmed_df)
    ## special case handling

    ## if the VDS site has just two lanes, and the paired site has
    ## more than two lanes, then the left lane at the paired site is
    ## unlikely to have truck variables in it, other than VDS data
    ## from the pairing.
    ##
    ## In that case, I *want* to have trucks in the left lane, as they
    ## are legally allowed there etc etc, whereas in the
    ## more-than-two-lanes original site they are not allowed in the
    ## left lane.  So, what the next bit of code does is to check
    ## first that there are 2 lanes at the VDS site, and that there
    ## are more than two lanes at the truck site.  Then it copies the
    ## truck variables into the trimmed_df, renaming the lane part
    ## from _r2 to _l1

    ## do this only if lanes at vds === 2
    if(length(lanes_vds) == 2){
        ## some contortions to drop "vds" lanes from the paired data
        ## so that I can have just the WIM laned data
        varnames <-  names(df)
        nhh_pattern <- 'not_heavyheavy'
        df_laned_vars <- grep(pattern=nhh_pattern,x=varnames,
                              perl=TRUE,value=TRUE,invert=FALSE)
        ## now, in the wim-only laned variables, are there more than 2 lanes?
        if(length(wim_unique_lanes)>1){
            print(paste('just two lanes in target vds data'
                       ,'and more than one lane in WIM data in merged set. '
                       ,'Re-using second lane from right at WIM site as'
                       ,'artificial left lane at paired VDS site'
                       ,sep=' '))
            ## okay, have something to do
            ## extract all wim data from lane r2

            ## get a list of laned variable names from trimmed_df in
            ## right lane only
            wim_right_lane2_pattern <- '_r2' ## only WIM data uses the _
            wim_right_lane2_vars <- grep(pattern=wim_right_lane2_pattern,
                                        x=names(df),
                                        perl=TRUE,value=TRUE,invert=FALSE)
            ## grab the rightlane2 WIM data from the merged wim/vds site
            df_wim_right <- df[,c(wim_right_lane2_vars,unlaned_vars)]
            rename_r2_l1 <- sub(pattern='_r2$',replacement='_l1',
                                x=names(df_wim_right),
                                perl=TRUE)
            ## do the rename
            names(df_wim_right) <- rename_r2_l1

            ## exclude those that already exist, which pretty much
            ## only means wgt_spd_all_veh_speed_l1 and
            ## count_all_veh_speed_l1 if those are already in the
            ## trimmed_df data.frame
            non_overlapping_l1 <- setdiff(rename_r2_l1,names(trimmed_df))
            expanded_df <- merge(trimmed_df,
                                 df_wim_right[,c(unlaned_vars,
                                                 non_overlapping_l1)],
                                 all=TRUE)
            trimmed_df <- expanded_df

        }
    }
    ## do this only if lanes at WIM === 2 and lanes at VDS > 2
    if(length(lanes_vds) > 2 && length(wim_unique_lanes) == 2){
        ## similar to the above block, but in this case rename left
        ## lane WIM data to lane r2
        ## now, in the wim-only laned variables, are there more than 2 lanes?
        print(paste('more than two lanes in target vds data'
                   ,'but just two lanes in WIM data in merged set. '
                   ,'make sure second lane in WIM merged set is renamed to r2'
                   ,'so as to match up with VDS site'
                   ,sep=' '))
        ## okay, have something to do
        ## extract all wim data from lane r2

        ## get a list of laned variable names from trimmed_df in
        ## right lane only
        wim_right_lane2_pattern <- '_r2' ## only WIM data uses the _
        wim_right_lane2_vars <- grep(pattern=wim_right_lane2_pattern,
                                     x=names(df),
                                     perl=TRUE,value=TRUE,invert=FALSE)

        wim_left_lane_pattern <- 'l1' ## only WIM data uses the _, but
                                      ## in this case I also want to
                                      ## snag the paired VDS data and
                                      ## make it nr2 and or2, instead of nl1, ol1

        wim_left_lane_vars <- grep(pattern=wim_left_lane_pattern,
                                   x=names(df),
                                   perl=TRUE,value=TRUE,invert=FALSE)

        print(paste('left lane vars',paste(wim_left_lane_vars,collapse=', '),sep=':'))
        if(length(wim_right_lane2_vars) == 0){
            ## no existing right 2 lane, so move left lane (l1) to right 2 (r2)
            ## grab the rightlane2 WIM data from the merged wim/vds site
            df_wim_left <- df[,c(wim_left_lane_vars,unlaned_vars)]
            rename_r2_l1 <- sub(pattern='l1$',replacement='r2',
                                x=names(df_wim_left),
                                perl=TRUE)
            ## do the rename
            names(df_wim_left) <- rename_r2_l1

            ## exclude those that already exist, which pretty much
            ## only means wgt_spd_all_veh_speed_l1 and
            ## count_all_veh_speed_l1 if those are already in the
            ## trimmed_df data.frame

            for(llv in wim_left_lane_vars){
                trimmed_df[,llv] <- NULL
            }

            non_overlapping_l1 <- setdiff(rename_r2_l1,names(trimmed_df))
            ## print("non_overlapping_l1")
            ## print(non_overlapping_l1)

            expanded_df <- merge(trimmed_df,
                                 df_wim_left[,c(unlaned_vars,
                                                non_overlapping_l1)],
                                 all=TRUE)
            trimmed_df <- expanded_df
            ##print('trimmed names')
            ##print(names(trimmed_df))
        }
    }

    trimmed_df
}
