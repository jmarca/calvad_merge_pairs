##' Load WIM VDS paired data
##'
##' This function will load up all of the WIM VDS paired datasets for
##' a given set of WIM site ids.  It will match the incoming numbers
##' of lanes as best as possible.  So if you have a site with 5 lanes,
##' as indicated by the "vds.vars" input parameter, then this will try
##' to use sites with 5 or more lanes.  If it can't find that, then it
##' will fall back on the site(s) with the most lanes.
##'
##' The goal is to make a uniform data set for the imputation step.
##' If the paired data sets are missing some lanes compared to the
##' target VDS site, then you'll have to remove those extra lanes
##' prior to calling Amelia, or Amelia will crash as it will have no
##' way to guess the most likely values for those lanes.  This is also
##' why I picked the strange lane numbering scheme.  A site with 4
##' lanes can be paired with a site with 3 lanes because left lanes
##' and right lanes are correct, and are labeled the same way.
##'
##' @title load.wim.pair.data
##' @param wim.pairs a list of pairings.  Each element in the list
##' should have a named element $vds_id, $wim_site, and $direction.
##' So if there are two pairings, the first between wim.51.E and vdsid
##' 318383, and the second between wim.52.W and vdsid 313822, then you
##' would pass a list something like
##' [{wim_site=51,direction='E',vds_id=318383}, and
##' {wim_site=52,direction='W',vds_id=313822}] (describing the list in
##' JSON notation because R sucks for concise notation of arrays of
##' hashmaps)
##' @param vds.nvars the VDS count variables from the target site,
##' used to limit the chosen set of matched WIM-VDS paired sites
##' @param year the year
##' @param db default "vdsdata\%2ftracking", the couchdb to save into
##' @return the "big data" dataframe of combined WIM and VDS sites,
##' trimmed to the right number of lanes
##' @author James E. Marca
##' @export
load.wim.pair.data <- function(wim.pairs,
                               vds.nvars,
                               year,
                               db){

    bigdata <- data.frame()
    wim_unique_lanes <- NULL
    vds_unique_lanes <- calvadrscripts::extract_unique_lanes(vds.nvars)

    spd.pattern <- "(^sl1$|^sr\\d$)"
    for(pairing in wim.pairs){
        print(paste('processing pairing',paste(pairing,collapse=' ')))
        paired.RData <- couch.get.merged.pair(trackingdb=db,
                                              vds.id=pairing$vds_id,
                                              wim.site=pairing$wim_site,
                                              direction=pairing$direction,
                                              year=year
                                              )
        if(dim(paired.RData)[1] < 100){
            print(paste('pairing for',pairing$vds_id,pairing$wim_site,'pretty empty'))
            next()
        }

        ## trim off some variables
        df.trimmed <- evaluate.paired.data(paired.RData
                                          ,vds.names=vds.nvars)

        df.trimmed$vds_id <- pairing$vds_id

        if(length(bigdata)==0){
            bigdata <-  df.trimmed
            df_wim_lanes <- grep(pattern='not_heavyheavy',x=names(df.trimmed),
                                  perl=TRUE,value=TRUE,invert=FALSE)
            wim_unique_lanes <- calvadrscripts::extract_unique_lanes(df_wim_lanes)

        }else{
            ic.names <- names(df.trimmed)
            bigdata.names <- names(bigdata)

            df_wim_lanes <- grep(pattern='not_heavyheavy',x=ic.names,
                                 perl=TRUE,value=TRUE,invert=FALSE)
            new_wim_unique_lanes <- calvadrscripts::extract_unique_lanes(df_wim_lanes)

            ## only merge if new_wim_unique_lanes > vds lanes OR == old wim_unique_lanes
            if(length(new_wim_unique_lanes) > length(vds_unique_lanes) ||
               length(new_wim_unique_lanes) ==  length(wim_unique_lanes)){

                print('merging new pair with old pair(s)')
                print(paste('vds lanes',paste(vds_unique_lanes,collapse=','),
                            'new wim pair lanes',paste(new_wim_unique_lanes,collapse=','),
                            'old wim pair lanes',paste(wim_unique_lanes,collapse=',')))
                ## here I need to make sure all WIM-VDS sites have similar lanes
                ## the concern is a site with *fewer* lanes than the vds site
                ## keep the larger of the two


                common.names <- intersect(ic.names,bigdata.names)
                bigdata <- bigdata[,common.names]
                df.trimmed <- df.trimmed[,common.names]
                bigdata <- rbind( bigdata, df.trimmed )
            }
            ## if not, don't merge
        }
    }

    bigdata
}
