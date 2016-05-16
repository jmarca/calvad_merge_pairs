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

    ##print(lanes_paired)
    ##print(lanes_vds)
    if(is.null(lanes_vds)){
        stop(paste('no vds lanes detected in passed vds.names:',vds.names))
    }

    dfnames <-  names(df)

    ## logic.
    ## Inspect the VDS-WIM paired data in df
    ## figure out the lanes with WIM-truck data
    ## figure out the lanes with VDS data (they don't have to be the same)
    ## make the best fit from df.vds lanes and df.wim lanes
    ## to the incoming vds.names list (extracted to lanes_vds)


    ## cases

    ## VDS 1
    ## ##
    ## ## WIM 1 : just make sure no extras in WIM-VDS paired
    ## ##
    ## ## WIM 2 : ditto above.  drop non-r1 data from wim site
    ## ##
    ## ## WIM 3+:  ditto above
    ## ##
    ## In VDS1 case, just make sure no non-r1 data in paired VDS-WIM data
    ## done in vds1_wimN

    ## VDS 2
    ## ## WIM 1 : Need to return r1 data, & no extra lanes in VDS-WIM site
    ## Handled by vdsN_wim1
    ##
    ## ##
    ## ## WIM 2 : Make sure exactly l1 r1 data in WIM-VDS paired site,
    ## ## handling special cases with VDS part of paired site possibly
    ## ## having more than two lanes so using r2 as l1 rather than the
    ## ## actual l1
    ## Handled by vds2_wim2
    ##
    ## ##
    ## ## WIM 3+: make sure returned data has just two lanes, not 3,
    ## ## using the R2 lane as l1 for both the WIM and the VDS data,
    ## ## dropping lane R3+ and lane L1 from the vds-wim paired data

    ## VDS 3+
    ## ##
    ## ## WIM 1 : just make sure no non-R1 data in VDS-WIM paired site
    ## Handled by vdsN_wim1
    ##
    ## ##
    ## ## WIM 2 : rename L1 data from WIM site to R2; make sure the
    ## ## VDS half of the WIM-VDS paired data has R2 data, otherwise
    ## ## also rename L1 to R2 for the VDS lanes of the WIM-VDS site
    ## ##
    ## ## WIM 3+: make sure the returned data has equivalent numbers
    ## ## of lanes as the VDS data, and that there are no extraneous
    ## ## VDS lanes compared to the WIM lanes in the VDS-WIM data.

    ## so methods:

    ## trim WIM lanes to a certain number...1 or 3+

    ## trim or rename WIM lanes in two-lane cases
    ## IF VDS==2 & WIM >2, trim to two lanes
    ## IF VDS >2 & WIM==2, rename L1 to R2

    ## trim or rename VDS lanes in two-lane cases
    ## IF VDS==2 & VDSW >2, Drop r3+, l1; rename r2 to l1
    ## IF VDS >2 & VDSW==2, rename L1 to R2


    ## match up VDS and WIM lanes to 1 or 3+
    ## given a number, drop all extra lane data
    ## does not do renaming.
    ## drops all L1 data.
    ##

    ## identify other data in the df, unrelated to lanes (things like
    ## time of day, nubmer of observations, time stamp, etc)
    laned_pattern <- paste(lanes_paired,'$',sep='',collapse='|')
    unlaned_vars <- grep(pattern=laned_pattern,x=dfnames,
                         perl=TRUE,value=TRUE,invert=TRUE)


    ## extract the WIM lanes...those lanes with Truck data
    nhh_pattern <- 'not_heavyheavy'
    df_wim_laned_vars <- grep(pattern=nhh_pattern,x=dfnames,
                          perl=TRUE,value=TRUE,invert=FALSE)
    wim_unique_lanes <- calvadrscripts::extract_unique_lanes(df_wim_laned_vars)


    ## extract the vds-based lane data in df.  by construction, this
    ## wil also include all of the WIM laned data to, because the
    ## pattern I generate below includes all lanes in the df data
    df_laned_vars <- grep(pattern=laned_pattern,x=dfnames,
                       perl=TRUE,value=TRUE)

    ## print('**************************************************')
    ## print(laned_pattern)
    ## print('**************************************************')
    ## print(df_laned_vars)

    ## print('**************************************************')
    # and this is the lanes in the DF that best match the incoming lanes
    laned_pattern <- paste(lanes_vds,'$',sep='',collapse='|')
    keep_theselaned_vars <- grep(pattern=laned_pattern,x=dfnames,
                                 perl=TRUE,value=TRUE)


    ## not sure yet why I'm doing this.  may not need anymore with
    ## this refactor

    ## keep_names <- c(laned_vars,unlaned_vars)
    ## trimmed_df <- df[,keep_names]

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
        if(length(wim_unique_lanes)>1){
            return_df <- vds2_wim2(df,
                                   df_laned_vars,
                                   keep_theselaned_vars,
                                   unlaned_vars)

            ## ## exclude those that already exist, which pretty much
            ## ## only means wgt_spd_all_veh_speed_l1 and
            ## ## count_all_veh_speed_l1 if those are already in the
            ## ## trimmed_df data.frame
            ## non_overlapping <- setdiff(names(incoming_df),names(trimmed_df))


            ## expanded_df <- merge(trimmed_df,
            ##                     ,
            ##                      all=TRUE)
            ## trimmed_df <- expanded_df
            return (return_df)
        }else{
            ## vds lanes is 2, wim lanes =1 (or zero)
            return_df <- vds2_wim1(df,
                                   df_laned_vars,
                                   keep_theselaned_vars,
                                   unlaned_vars)
            ## post
            return(return_df)
        }
    }

    ## still here? other cases
    if(length(lanes_vds)>2){
        if(length(wim_unique_lanes)>1){
            return_df <- vds3_wim2(df,
                                   df_laned_vars,
                                   keep_theselaned_vars,
                                   unlaned_vars)
            ## post
            return(return_df)


        }else{
            return_df <- vds3_wim1(df,
                                   df_laned_vars,
                                   keep_theselaned_vars,
                                   unlaned_vars)
            ## post
            return(return_df)
        }

    }

    if(length(lanes_vds) == 1){
        return_df <- vds1_wimN(df,
                               df_laned_vars,
                               keep_theselaned_vars,
                               unlaned_vars)
        ## post
        return(return_df)
    }else{
        ## I don't know.  if it isn't 1, 2, or >2, what else can it be?
        stop(paste('problem value for length of VDS site lanes vector',length(lanes_vds)))
    }

}
##' One VDS lane, any number WIM lanes
##'
##' In this case, the incoming VDS site has one lane.  So just pull
##' out the right lane from WIM paired data
##'
##' @title vds1_wimN
##' @param df the data frame from the vds-wim paired site
##' @param df_wim_laned_vars the variables for the WIM data in the
##'     paired df
##' @param df_laned_vars all of the laned variables in the paired df,
##'     which includes the WIM data and the VDS paired data.
##' @param keep_theselaned_vars this is the laned data in the incoming
##'     VDS site, against which I am trying to match the vds-wim
##'     paired site
##' @param unlaned_vars variables in the df unrelated to lane-based
##'     data that I should keep around in the result.  Things like
##'     time stamp, number of observations, etc
##' @return a data frame with lane data matching up against the
##'     incomig VDS variables
##' @author James E. Marca
vds1_wimN <- function(df,
                      df_wim_laned_vars,
                      df_laned_vars,
                      keep_theselaned_vars,
                      unlaned_vars){
    ## in this case, just make sure no non-r1 data in df

    right_lane1_pattern <- 'r1' ## only WIM data uses the _ ,
                                ## plain r1 grabs both
    right_lane1_vars <- grep(pattern=right_lane1_pattern,
                                 x=df_laned_vars,
                             perl=TRUE,value=TRUE,invert=FALSE)

    return (df[,c(right_lane1_vars,unlaned_vars)])

}

##' Any number of VDS lanes, exactly one WIM lane
##'
##' In this case, the paired WIM-VDS site has just one WIM lane.  So
##' make sure there aren't extra VDS lanes, and spit that right lane
##' out
##'
##' this is an alias to VDSN_wim1, because one lane is one lane.  All
##' I'm doing is making sure that there are no extraneous lanes by
##' select just r1
##'
##' @title vdsN_wim1
##' @param df the data frame from the vds-wim paired site
##' @param df_wim_laned_vars the variables for the WIM data in the
##'     paired df
##' @param df_laned_vars all of the laned variables in the paired df,
##'     which includes the WIM data and the VDS paired data.
##' @param keep_theselaned_vars this is the laned data in the incoming
##'     VDS site, against which I am trying to match the vds-wim
##'     paired site
##' @param unlaned_vars variables in the df unrelated to lane-based
##'     data that I should keep around in the result.  Things like
##'     time stamp, number of observations, etc
##' @return a data frame with lane data matching up against the
##'     incomig VDS variables
##' @author James E. Marca
vdsN_wim1 <- vds1_wimN



##' two VDS lanes, and just two WIM lanes
##'
##' VDS2 & WIM 2 : Make sure exactly l1 r1 data in WIM-VDS paired site,
##' handling special cases with VDS part of paired site possibly
##' having more than two lanes so using r2 as l1 rather than the
##' actual l1
##'
##' The WIM VDS pairing tries not to mess around with the variable
##' names when the pairing is done.  This means that you can get
##' situations like the paired VDS site has three lanes, but the paird
##' WIM site has just two, and therefore you will see things like
##' heavyheavy_r1 and heavyheavy_l1 sharing space with nr1,nr2,nl1.
##'
##' So with that in mind, this case handles the situation in which the
##' *to be analyzed* VDS site (not the one that has been paired to a
##' WIM station) has more than two lanes, the WIM site in the paired
##' set has just two lanes, but the VDS paired to the WIM site *might*
##' have more than two lanes (that sometimes happens).
##'
##' So in that case, what I want to do for the imputation of trucks at
##' the incoming VDS site is to keep the left lane for WIM
##' site (all the _l1 variables) as l1 variables, but shift the possible
##' "middle lane" (or r2) type variables from the paired VDS stations to
##' be l1 variables, and *if* it is the case that there are r2 variables,
##' drop the paired vds l1 variables.
##'
##' This function does that renaming of variables for this case
##'
##' @title two_vds_lanes_two_wim_lanes
##' @param df_laned_vars all of the laned variables in the paired df,
##'     which includes the WIM data and the VDS paired data.
##' @param keep_theselaned_vars this is the laned data in the incoming
##'     VDS site, against which I am trying to match the vds-wim
##'     paired site
##' @param unlaned_vars variables in the df unrelated to lane-based
##'     data that I should keep around in the result.  Things like
##'     time stamp, number of observations, etc
##' @return a data frame with lane data matching up against the
##'     incomig VDS variables
##' @author James E. Marca
vds2_wim2 <- function(df,
                      df_laned_vars,
                      keep_theselaned_vars,
                      unlaned_vars){

    ## borkborkbork()

    print(paste('vds2_wim2'
               ,'just two lanes in target vds data'
               ,'and more than one lane in WIM data in merged set. '
               ,'Re-using second lane from right at WIM site as'
               ,'artificial left lane at paired VDS site'
               ,sep=' '))


    ## going to keep all r1 vars
    right_lane1_pattern <- 'r1' ## only WIM data uses the _ ,
                                    ## so be careful here to grab both
    right_lane1_vars <- grep(pattern=right_lane1_pattern,
                                 x=df_laned_vars,
                                 perl=TRUE,value=TRUE,invert=FALSE)

    ## I know there are exactly 2 WIM lanes, but perhaps 3 or more vds lanes

    ## keep the truck l1 lanes

    left_lane_pattern <- 'l1'
    left_lane_vars <- grep(pattern=left_lane_pattern,
                               x=df_laned_vars,
                               perl=TRUE,value=TRUE,invert=FALSE)

    ## if the paired VDS half of df has any R2 lanes, use them, but
    ## rename them as l1
    right_lane2_pattern <- 'r2' ## safe to skip '_' because no wim r2 data
    right_lane2_vars <- grep(pattern=right_lane2_pattern,
                             x=df_laned_vars,
                             perl=TRUE,value=TRUE,invert=FALSE)
    if(length(right_lane2_vars) > 0){
        ## print(right_lane2_vars)
        ## have r2 lanes, so rename and use as l1 data
        print(left_lane_vars)

        return_df <- df[,c(right_lane1_vars,
                           left_lane_vars,
                           unlaned_vars)]

        rename_lane2 <- sub(pattern='r2$',replacement='l1',
                            x=right_lane2_vars, ## or names(return_df)
                            perl=TRUE)

        return_df[,rename_lane2] <- df[,right_lane2_vars]

        return (return_df)

    }else{
        print('no right2 lanes')
        return (df[,c(right_lane1_vars,
                      left_lane_vars,
                      unlaned_vars)]
                )

    }
}


##' Two VDS lanes, and more than two WIM lanes
##'
##' The WIM VDS pairing tries not to mess around with the variable
##' names when the pairing is done.  This means that you can get
##' situations like the paired VDS site has three lanes, but the paird
##' WIM site has just two, and therefore you will see things like
##' heavyheavy_r1 and heavyheavy_l1 sharing space with nr1,nr2,nl1.
##'
##' So with that in mind, this case handles the situation in which the
##' *incoming* VDS site (not the one that has been paired to a WIM
##' station) has more than two lanes, but the WIM site in the paired
##' set has just two lanes, which is indicated by having _r1 and _l1
##' variables, but NOT _r2 variables.
##'
##' So in that case, what I want to do for the imputation of trucks at
##' the incoming VDS site is to pretend that the left lane for the WIM
##' site (all the _l1 variables) really should be _r2 variables.
##' Because in a three lane highway, heavy heavy trucks and most not
##' hh trucks are denied access to that left most lane by law, whereas
##' in a two lane case they can use the left lane to pass.
##'
##' This function does that renaming of variables for this case
##'
##' @title vds2_wim3
##' @param df the wim_vds paired dataframe
##' @param trimmed_df just the DF with WIM data
##' @param laned_vars the variables relating to lanes
##' @param unlaned_vars the variables relating to other stuff than
##'     lanes...the time of day, observation counts and the like
##' @return a renamed and suitably trimmed wim_vds merged set
##' @author James E. Marca
vds2_wim3 <- function(df,trimmed_df,laned_vars,unlaned_vars){

    varnames <-  names(df)

    nhh_pattern <- 'not_heavyheavy'
    df_laned_vars <- grep(pattern=nhh_pattern,x=varnames,
                          perl=TRUE,value=TRUE,invert=FALSE)

    print(paste('just two lanes in target vds data'
               ,'and more than two lanes in WIM data in merged set. '
               ,'Re-using second lane from right at WIM site as'
               ,'artificial left lane to match up the target VDS site'
               ,sep=' '))

    ## get a list of laned variable names from trimmed_df in
    ## right lane only
    right_lane1_pattern <- 'r1' ## only WIM data uses the _ ,
                                    ## so be careful here to grab both
    right_lane1_vars <- grep(pattern=right_lane1_pattern,
                                 x=names(df),
                                 perl=TRUE,value=TRUE,invert=FALSE)

    ## now the right 2 vds variables, if any
    ## if there is an r2 set, then use it
    vds_right_lane_2_pattern <- '[^_]r2'
    vds_right_2_vars <-  grep(pattern=vds_right_lane2_pattern,
                                 x=names(df),
                                 perl=TRUE,value=TRUE,invert=FALSE)

    print('right 2 vars debug')
    print(vds_right_2_vars)

    ## now the left lane truck vars, maybe with VDS vars
    wim_left_lane_pattern <- '_l1'
    if(length(vds_right_2_vars) == 0){
        ## in this case, use 'l1' not '_l1' to pick up VDS l1 vars
        ## because there aren't any r2 vars
        wim_left_lane_pattern <- 'l1'
    }
    wim_left_vars <-  grep(pattern=wim_left_lane_pattern,
                           x=names(df),
                           perl=TRUE,value=TRUE,invert=FALSE)

    print('wim left vars debug')
    print(wim_left_vars)


    ## (don't forget to rename the left lane vars below to r2)

    ## stash out all the variables that I am keeping.  two lanes worth

    df_reduced <- df[,c(wim_right_lane1_vars,wim_left_vars,unlaned_vars)]

    ## now for the renaming step, identify the left lane stuff

    rename_l1_r2 <- sub(pattern='l1$',replacement='r2',
                        x=names(df_reduced),
                        perl=TRUE)


    ## do the rename
    names(df_reduced) <- rename_l1_r2

    print('are the finished names renamed correctly, with no more l1 values?')
    print(names(df_reduced))


    return (df_reduced)

}


##' Three or more VDS lanes, exactly two WIM lanes
##'
##' In this case, the incoming VDS site has more than two lanes, and
##' the WIM site has two or more lanes, but maybe more paired vds
##' lanes.
##' @title vds3_wim2
##' @param df the data frame from the vds-wim paired site
##' @param df_wim_laned_vars the variables for the WIM data in the
##'     paired df
##' @param df_laned_vars all of the laned variables in the paired df,
##'     which includes the WIM data and the VDS paired data.
##' @param keep_theselaned_vars this is the laned data in the incoming
##'     VDS site, against which I am trying to match the vds-wim
##'     paired site
##' @param unlaned_vars variables in the df unrelated to lane-based
##'     data that I should keep around in the result.  Things like
##'     time stamp, number of observations, etc
##' @return a data frame with lane data matching up against the
##'     incomig VDS variables
##' @author James E. Marca
vds3_wim2 <- function(df,
                      df_wim_laned_vars,
                      df_laned_vars,
                      keep_theselaned_vars,
                      unlaned_vars){

    borkborkbork()
}

##     ## do this only if lanes at WIM == 2 and lanes at VDS > 2
## ##    if(length(lanes_vds) > 2 && length(wim_unique_lanes) == 2){
## ## similar to the above block, but in this case rename left
##         ## lane WIM data to lane r2
##         ## now, in the wim-only laned variables, are there more than 2 lanes?
##     ## incoming_df <-
##         print(paste('more than two lanes in target vds data'
##                    ,'but just two lanes in WIM data in merged set. '
##                    ,'make sure second lane in WIM merged set is renamed to r2'
##                    ,'so as to match up with VDS site'
##                    ,sep=' '))
##         ## okay, have something to do
##         ## extract all wim data from lane r2

##         ## get a list of laned variable names from trimmed_df in
##         ## right lane only
##         wim_right_lane2_pattern <- '_r2' ## only WIM data uses the _
##         wim_right_lane2_vars <- grep(pattern=wim_right_lane2_pattern,
##                                      x=dfnames,
##                                      perl=TRUE,value=TRUE,invert=FALSE)

##         wim_left_lane_pattern <- 'l1' ## only WIM data uses the _, but
##                                       ## in this case I also want to
##                                       ## snag the paired VDS data and
##                                       ## make it nr2 and or2, instead of nl1, ol1

##         wim_left_lane_vars <- grep(pattern=wim_left_lane_pattern,
##                                    x=dfnames,
##                                    perl=TRUE,value=TRUE,invert=FALSE)

##         print(paste('left lane vars',paste(wim_left_lane_vars,collapse=', '),sep=':'))
##         if(length(wim_right_lane2_vars) == 0){
##             ## no existing right 2 lane, so move left lane (l1) to right 2 (r2)
##             ## grab the rightlane2 WIM data from the merged wim/vds site
##             df_wim_left <- df[,c(wim_left_lane_vars,unlaned_vars)]
##             rename_r2_l1 <- sub(pattern='l1$',replacement='r2',
##                                 x=names(df_wim_left),
##                                 perl=TRUE)
##             ## do the rename
##             print(rename_r2_l1)
##             print(names(df_wim_left))
##             names(df_wim_left) <- rename_r2_l1
##             print(names(df_wim_left))
##             ## exclude those that already exist, which pretty much
##             ## only means wgt_spd_all_veh_speed_l1 and
##             ## count_all_veh_speed_l1 if those are already in the
##             ## trimmed_df data.frame

##             for(llv in wim_left_lane_vars){
##                 trimmed_df[,llv] <- NULL
##             }

##             non_overlapping_l1 <- setdiff(rename_r2_l1,names(trimmed_df))
##             ## print("non_overlapping_l1")
##             ## print(non_overlapping_l1)

##             expanded_df <- merge(trimmed_df,
##                                  df_wim_left[,c(unlaned_vars,
##                                                 non_overlapping_l1)],
##                                  all=TRUE)
##             trimmed_df <- expanded_df
##             ##print('trimmed names')
##             ##print(names(trimmed_df))
##         }
##     }

##     trimmed_df
## }
