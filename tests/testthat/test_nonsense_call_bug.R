config <- rcouchutils::get.config(Sys.getenv('RCOUCHUTILS_TEST_CONFIG'))
parts <- c('wim','vds','borkedcall')
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
    "will not crash with screwy parameters",{
    context("totally broken")
    wim.pairs <- list()
    wim.pairs[[1]] <- list(vds_id=NA,wim_site=NA,direction=NA)
    bigdata <- load.wim.pair.data(wim.pairs=wim.pairs,
                                  vds.nvars=c('nl1','nr3','nr2','nr1'),
                                  year=2012,
                                  db=parts
                                  )

    expect_that(bigdata,is_a('data.frame'))
    expect_that(dim(bigdata),equals(c(0,0)))


    wim.pairs[[2]] <- list(vds_id=318383,wim_site=51,direction='E')
    bigdata <- load.wim.pair.data(wim.pairs=wim.pairs,
                                  vds.nvars=c('nl1','nr3','nr2','nr1'),
                                  year=2012,
                                  db=parts
                                  )

    expect_that(bigdata,is_a('data.frame'))

    expect_that(dim(bigdata),equals(c(1820,51)))
    expect_that(sort(unique(bigdata$vds_id)),equals(c(318383)))

})

rcouchutils::couch.deletedb(parts)
