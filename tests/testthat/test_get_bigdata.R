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
    "can merge together merged files",{
    wim.pairs <- list()
    wim.pairs[[1]] <- list(vds_id=313822,wim_site=52,direction='W')
    bigdata <- load.wim.pair.data(wim.pairs=wim.pairs,
                                  vds.nvars=c('nl1','nr3','nr2','nr1'),
                                  year=2012,
                                  db=parts
                                  )

    expect_that(bigdata,is_a('data.frame'))

    expect_that(dim(bigdata),equals(c(8724,49)))
    ##print(names(bigdata))
    wim.pairs[[2]] <- list(vds_id=318383,wim_site=51,direction='E')
    bigdata <- load.wim.pair.data(wim.pairs=wim.pairs,
                                  vds.nvars=c('nl1','nr3','nr2','nr1'),
                                  year=2012,
                                  db=parts
                                  )

    expect_that(bigdata,is_a('data.frame'))

    expect_that(dim(bigdata),equals(c(10544,49)))
    expect_that(sort(unique(bigdata$vds_id)),equals(c(313822,318383)))
})
