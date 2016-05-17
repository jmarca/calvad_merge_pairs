config <- rcouchutils::get.config(Sys.getenv('RCOUCHUTILS_TEST_CONFIG'))
path <- './files'
year <- 2012

## I need a wimvds paired file with 3+ lanes, 2 lanes, 1 lane
## to properly exercise all alternatives

wim_files <- c('./files/wim.98.N.vdsid.812776.2012.paired.RData',
               './files/wim.100.S.vdsid.1108721.2012.paired.RData'
               'files/wim.98.S.vdsid.819321.2012.paired.RData',
               'files/wim.96.W.vdsid.801377.2012.paired.RData')
vds_file <- 'files/801331_ML_2012.120.imputed.RData'

test_that(
    "correctly fix to the same number of lanes for wim site with 2 lanes",{
    env <- new.env()
    res <- load(file=wim_files[1],envir=env)
    already_merged <- env[[res]]


    expect_that(dim(already_merged),equals(c(8775,34)))


    vds.names <- c('nr1')
    new_df <- evaluate.paired.data(already_merged,vds.names)
    expect_that(dim(new_df),equals(c(8775,18)))
    expect_that(new_df[,'nr1'],equals(already_merged[,'nr1']))

    vds.names <- c('nr1','nl1')
    new_df <- evaluate.paired.data(already_merged,vds.names)
    expect_that(dim(new_df),equals(c(8775,32)))
    expect_that(new_df[,'nr1'],equals(already_merged[,'nr1']))
    expect_that(new_df[,'nl1'],equals(already_merged[,'nr2']))
    expect_that(new_df[,'heavyheavy_r1'],equals(already_merged[,'heavyheavy_r1']))
    expect_that(new_df[,'heavyheavy_l1'],equals(already_merged[,'heavyheavy_r2']))

    vds.names <- c('nr1','nr2','nl1')
    new_df <- evaluate.paired.data(already_merged,vds.names)
    expect_that(dim(new_df),equals(dim(already_merged)))
    expect_that(new_df[,'nr1'],equals(already_merged[,'nr1']))
    expect_that(new_df[,'nr2'],equals(already_merged[,'nr2']))
    expect_that(new_df[,'nl1'],equals(already_merged[,'nl1']))
    expect_that(new_df[,'heavyheavy_r1'],equals(already_merged[,'heavyheavy_r1']))
    expect_that(new_df[,'heavyheavy_r2'],equals(already_merged[,'heavyheavy_l1']))


    vds.names <- c('nr1','nr2','nr3','nl1')
    newnew_df <- evaluate.paired.data(already_merged,vds.names)
    expect_that(length(newnew_df$nr3),equals(0))
    expect_that(new_df,equals(newnew_df))

})

test_that(
    "correctly fix to the same number of lanes for wim site with 3 lanes",{
    env <- new.env()
    res <- load(file=wim_files[2],envir=env)
    already_merged <- env[[res]]


    expect_that(dim(already_merged),equals(c(8775,54))) ## more lanes
    testthat::expect_false('heavyheavy_l1' %in% names(already_merged))


    vds.names <- c('nr1')
    new_df <- evaluate.paired.data(already_merged,vds.names)
    expect_that(dim(new_df),equals(c(8775,18)))
    expect_that(new_df[,'nr1'],equals(already_merged[,'nr1']))

    vds.names <- c('nr1','nl1')
    new_df <- evaluate.paired.data(already_merged,vds.names)
    expect_that(dim(new_df),equals(c(8775,32)))
    expect_that(new_df[,'nr1'],equals(already_merged[,'nr1']))
    expect_that(new_df[,'nl1'],equals(already_merged[,'nr2']))
    expect_that(new_df[,'heavyheavy_r1'],equals(already_merged[,'heavyheavy_r1']))
    expect_that(new_df[,'heavyheavy_l1'],equals(already_merged[,'heavyheavy_r2']))

    vds.names <- c('nr1','nr2','nl1')
    new_df <- evaluate.paired.data(already_merged,vds.names)
    expect_that(dim(new_df),equals(c(8775,46)))
    expect_that(new_df[,'nr1'],equals(already_merged[,'nr1']))
    expect_that(new_df[,'nr2'],equals(already_merged[,'nr2']))
    expect_that(new_df[,'heavyheavy_r1'],equals(already_merged[,'heavyheavy_r1']))
    expect_that(new_df[,'heavyheavy_r2'],equals(already_merged[,'heavyheavy_r2']))
    ## should have renamed r3 in pair set to l1
    expect_that(new_df[,'nl1'],equals(already_merged[,'nr3']))
    expect_that(new_df[,'heavyheavy_l1'],equals(already_merged[,'heavyheavy_r3']))


    vds.names <- c('nr1','nr2','nr3','nl1')
    new_df <- evaluate.paired.data(already_merged,vds.names)
    expect_that(dim(new_df),equals(c(8775,50)))
    expect_that(new_df[,'nr1'],equals(already_merged[,'nr1']))
    expect_that(new_df[,'nr2'],equals(already_merged[,'nr2']))
    expect_that(new_df[,'heavyheavy_r1'],equals(already_merged[,'heavyheavy_r1']))
    expect_that(new_df[,'heavyheavy_r2'],equals(already_merged[,'heavyheavy_r2']))
    expect_that(new_df[,'nr3'],equals(already_merged[,'nr3']))
    expect_that(new_df[,'heavyheavy_r3'],equals(already_merged[,'heavyheavy_r3']))
    ## should have used paired set l1 as the new df l1 columns
    ## because wim/truck data in paired set tops out at r3
    expect_that(new_df[,'nl1'],equals(already_merged[,'nl1']))
    testthat::expect_false('heavyheavy_l1' %in% names(new_df))


    vds.names <- c('nr1','nr2','nr3','nr4','nl1')
    new_df <- evaluate.paired.data(already_merged,vds.names)
    expect_that(dim(new_df),equals(c(8775,54)))
    expect_that(new_df[,'nr1'],equals(already_merged[,'nr1']))
    expect_that(new_df[,'nr2'],equals(already_merged[,'nr2']))
    expect_that(new_df[,'heavyheavy_r1'],equals(already_merged[,'heavyheavy_r1']))
    expect_that(new_df[,'heavyheavy_r2'],equals(already_merged[,'heavyheavy_r2']))
    expect_that(new_df[,'nr3'],equals(already_merged[,'nr3']))
    expect_that(new_df[,'heavyheavy_r3'],equals(already_merged[,'heavyheavy_r3']))
    ## should have used paired set r4 as the new df r4 cols, but
    ## without any truck data in r4 because wim/truck data in paired
    ## set tops out at r3
    expect_that(new_df[,'nr4'],equals(already_merged[,'nr4']))
    testthat::expect_false('heavyheavy_r4' %in% names(new_df))
    ## should have used existing l1 as the new df l1 columns
    expect_that(new_df[,'nl1'],equals(already_merged[,'nl1']))
    testthat::expect_false('heavyheavy_l1' %in% names(new_df))

})

rcouchutils::couch.deletedb(parts)
