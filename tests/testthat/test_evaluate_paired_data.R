config <- rcouchutils::get.config(Sys.getenv('RCOUCHUTILS_TEST_CONFIG'))
path <- './files'
year <- 2012

file <- './files/wim.51.E.vdsid.318383.2012.paired.RData'

test_that(
    "same number of lanes",{
    env <- new.env()
    res <- load(file=file,envir=env)
    already_merged <- env[[res]]

    expect_that(dim(already_merged),equals(c(1820,50)))

    vds.names <- c('nr1','nr2','nr3','nl1')
    new_df <- evaluate.paired.data(already_merged,vds.names)
    expect_that(dim(new_df),equals(dim(already_merged)))

    vds.names <- c('nr1','nr2','nl1')
    new_df <- evaluate.paired.data(already_merged,vds.names)
    expect_that(dim(new_df),equals(c(1820,36)))

    vds.names <- c('nr1','nl1')
    new_df <- evaluate.paired.data(already_merged,vds.names)
    expect_that(dim(new_df),equals(c(1820,32)))


})

rcouchutils::couch.deletedb(parts)
