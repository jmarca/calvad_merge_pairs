config <- rcouchutils::get.config(Sys.getenv('RCOUCHUTILS_TEST_CONFIG'))
parts <- c('wim','vds','bigdata')
rcouchutils::couch.makedb(parts)
path <- './files'
year <- 2012

file <- './files/wim.51.E.vdsid.318383.2012.paired.RData'
couch.put.merged.pair(trackingdb=parts,
                      vds.id=318383,
                      file=file)
file <- './files/wim.52.W.vdsid.313822.2012.paired.RData'
couch.put.merged.pair(trackingdb=parts,
                      vds.id=313822,
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

    expect_that(dim(bigdata),equals(c(8724,49)))
    wim.pairs[[2]] <- list(vds_id=318383,wim_site=51,direction='E')
    bigdata <- load.wim.pair.data(wim.pairs=wim.pairs,
                                  vds.nvars=c('nl1','nr3','nr2','nr1'),
                                  year=2012,
                                  db=parts
                                  )

    expect_that(bigdata,is_a('data.frame'))

    expect_that(dim(bigdata),equals(c(10544,49)))
    expect_that(sort(unique(bigdata$vds_id)),equals(c(313822,318383)))

    context("just two lanes at vds site")
    bigdata <- load.wim.pair.data(wim.pairs=wim.pairs,
                                  vds.nvars=c('nl1','nr1'),
                                  year=2012,
                                  db=parts
                                  )

    expect_that(bigdata,is_a('data.frame'))
    expect_that(dim(bigdata),equals(c(10544,21)))
    expect_that(sort(names(bigdata)),
                equals(
                    c(
                        count_all_veh_speed_r1,
                        day,
                        heavyheavy_r1,
                        hh_axles_r1,
                        hh_len_r1,
                        hh_speed_r1,
                        hh_weight_r1,
                        nh_axles_r1,
                        nh_len_r1,
                        nh_speed_r1,
                        nh_weight_r1,
                        nl1, not_heavyheavy_r1,
                        nr1,
                        obs_count,
                        ol1,
                        or1,
                        tod, ts, vds_id,
                        wgt_spd_all_veh_speed_r1
                    )
                ))
    expect_that(sort(unique(bigdata$vds_id)),equals(c(313822,318383)))

})

rcouchutils::couch.deletedb(parts)
