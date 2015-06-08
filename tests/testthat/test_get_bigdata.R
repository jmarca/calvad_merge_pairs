config <- rcouchutils::get.config(Sys.getenv('RCOUCHUTILS_TEST_CONFIG'))
parts <- c('wim','vds','bigdata')

rcouchutils::couch.makedb(parts)
path <- './files'
year <- 2012

file <- paste(path,'wim.51.E.vdsid.318383.2012.paired.RData',sep='/')
couch.put.merged.pair(trackingdb=parts,
                      vds.id=318383,
                      file=file)
file <- paste(path,'wim.52.W.vdsid.313822.2012.paired.RData',sep='/')
couch.put.merged.pair(trackingdb=parts,
                      vds.id=313822,
                      file=file)


file <- paste(path,'wim.46.N.vdsid.317652.2012.paired.RData',sep='/')
calvadmergepairs::couch.put.merged.pair(trackingdb=parts,
                      vds.id=317652,
                      file=file)
file <- paste(path,'wim.46.S.vdsid.317656.2012.paired.RData',sep='/')
calvadmergepairs::couch.put.merged.pair(trackingdb=parts,
                      vds.id=317656,
                      file=file)


test_that(
    "can retrieve merged files",{
    context("all the data")
    wim.pairs <- list()
    wim.pairs[[1]] <- list(vds_id=313822,wim_site=52,direction='W')
    bigdata <- load.wim.pair.data(wim.pairs=wim.pairs,
                                  vds.nvars=c('nl1','nr3','nr2','nr1'),
                                  year=2012,
                                  db=parts
                                  )

    expect_that(bigdata,is_a('data.frame'))

    expect_that(dim(bigdata),equals(c(8724,51)))
        lane_l1_vars <- grep(pattern='l1$',x=names(bigdata),perl=TRUE,value=TRUE)
        lane_r1_vars <- grep(pattern='r1$',x=names(bigdata),perl=TRUE,value=TRUE)
        lane_r2_vars <- grep(pattern='r2$',x=names(bigdata),perl=TRUE,value=TRUE)
        lane_r3_vars <- grep(pattern='r3$',x=names(bigdata),perl=TRUE,value=TRUE)
        expect_that(length(lane_l1_vars),equals(4))
        expect_that(length(lane_r1_vars),equals(14))
        expect_that(length(lane_r2_vars),equals(14))
        expect_that(length(lane_r3_vars),equals(14))

    wim.pairs[[2]] <- list(vds_id=318383,wim_site=51,direction='E')
    bigdata <- load.wim.pair.data(wim.pairs=wim.pairs,
                                  vds.nvars=c('nl1','nr3','nr2','nr1'),
                                  year=2012,
                                  db=parts
                                  )

    expect_that(bigdata,is_a('data.frame'))

    expect_that(dim(bigdata),equals(c(10544,51)))
    expect_that(sort(unique(bigdata$vds_id)),equals(c(313822,318383)))

    context("just two lanes at vds site")
    bigdata <- load.wim.pair.data(wim.pairs=wim.pairs,
                                  vds.nvars=c('nl1','nr1'),
                                  year=2012,
                                  db=parts
                                  )

    expect_that(bigdata,is_a('data.frame'))
    expect_that(dim(bigdata),equals(c(10544,33)))
    expect_that(sort(names(bigdata)),
                equals(
                    c(
                        'count_all_veh_speed_l1',
                        'count_all_veh_speed_r1',
                        'day',
                        'heavyheavy_l1','heavyheavy_r1',
                        'hh_axles_l1',  'hh_axles_r1',
                        'hh_len_l1',    'hh_len_r1',
                        'hh_speed_l1',  'hh_speed_r1',
                        'hh_weight_l1', 'hh_weight_r1',
                        'nh_axles_l1',  'nh_axles_r1',
                        'nh_len_l1',    'nh_len_r1',
                        'nh_speed_l1',  'nh_speed_r1',
                        'nh_weight_l1', 'nh_weight_r1',
                        'nl1',
                        'not_heavyheavy_l1',
                        'not_heavyheavy_r1',
                        'nr1',
                        'obs_count',
                        'ol1',
                        'or1',
                        'tod', 'ts', 'vds_id',
                        'wgt_spd_all_veh_speed_l1',
                        'wgt_spd_all_veh_speed_r1'
                    )
                ))

    expect_that(sort(unique(bigdata$vds_id)),equals(c(313822,318383)))

})

test_that(
    "can generate two merged lanes from three vds lanes",{

        wim.pairs <- list()
        wim.pairs[[1]] <- list(vds_id=313822,wim_site=52,direction='W')
        wim.pairs[[2]] <- list(vds_id=318383,wim_site=51,direction='E')
        wim.pairs[[3]] <- list(vds_id=317652,wim_site=46,direction='N')
        wim.pairs[[4]] <- list(vds_id=317656,wim_site=46,direction='S')

        bigdata <- calvadmergepairs::load.wim.pair.data(wim.pairs=wim.pairs,
                                                        vds.nvars=c('nl1','nr2','nr1'),
                                                        year=2012,
                                                        db=parts
                                                        )

        expect_that(bigdata,is_a('data.frame'))
        ## expect_that(dim(bigdata),equals(c(28112,33)))
        print(dim(bigdata))
        ## print('names bigdata')
        ## print(names(bigdata))
        lane_l1_vars <- grep(pattern='l1$',x=names(bigdata),perl=TRUE,value=TRUE)
        lane_r1_vars <- grep(pattern='r1$',x=names(bigdata),perl=TRUE,value=TRUE)
        lane_r2_vars <- grep(pattern='r2$',x=names(bigdata),perl=TRUE,value=TRUE)
        lane_r3_vars <- grep(pattern='r3$',x=names(bigdata),perl=TRUE,value=TRUE)
        expect_that(length(lane_l1_vars),equals(0))
        expect_that(length(lane_r1_vars),equals(14))
        expect_that(length(lane_r2_vars),equals(14))
        expect_that(length(lane_r3_vars),equals(0))

        expect_that(sort(unique(bigdata$vds_id)),equals(sort(c(317652,317656,313822,318383))))

        bigdata <- calvadmergepairs::load.wim.pair.data(wim.pairs=wim.pairs,
                                                        vds.nvars=c('nl1','nr3','nr2','nr1'),
                                                        year=2012,
                                                        db=parts
                                                        )

        expect_that(bigdata,is_a('data.frame'))
        ## expect_that(dim(bigdata),equals(c(28112,33)))
        print(dim(bigdata))
        ## print('names bigdata')
        ## print(names(bigdata))
        lane_l1_vars <- grep(pattern='l1$',x=names(bigdata),perl=TRUE,value=TRUE)
        lane_r1_vars <- grep(pattern='r1$',x=names(bigdata),perl=TRUE,value=TRUE)
        lane_r2_vars <- grep(pattern='r2$',x=names(bigdata),perl=TRUE,value=TRUE)
        lane_r3_vars <- grep(pattern='r3$',x=names(bigdata),perl=TRUE,value=TRUE)
        expect_that(length(lane_l1_vars),equals(4))
        expect_that(length(lane_r1_vars),equals(14))
        expect_that(length(lane_r2_vars),equals(14))
        expect_that(length(lane_r3_vars),equals(14))



    })

rcouchutils::couch.deletedb(parts)
