config <- rcouchutils::get.config(Sys.getenv('RCOUCHUTILS_TEST_CONFIG'))
parts <- c('wim','vds','merge')
rcouchutils::couch.makedb(parts)
path <- './files'
direction <- 'N'
wim.site <- 1
year <- 2012
vds.id <- 1073210
filepath <- make.merged.filepath(vds.id,year,wim.site,direction,path)

test_that(
    "can merge vds and wim",{
        df.wim.imputed <- calvadrscripts::get.amelia.wim.file.local(
            site_no=wim.site
           ,year=year
           ,direction=direction
           ,path=path)
        df.wim.merged <- calvadrscripts::condense.amelia.output(df.wim.imputed)
        df.merged <- merge_wim_with_vds(df.wim.merged=df.wim.merged
                                       ,wim.site=wim.site
                                       ,direction=direction
                                       ,vds.id=vds.id
                                       ,path=path
                                       ,year=year
                                       ,trackingdb=parts
                                        )

        ## save to filesystem so to write to couchdb
        save(df.merged,file=filepath[1],compress='xz')

        ## attach to couchdb for convenience of use
        couch.put.merged.pair(trackingdb=parts,
                              vds.id=vds.id,
                              file=filepath[1])

        ## it should be in couchdb

        have_one <- couch.has.merged.pair(trackingdb=parts
                                         ,vds.id=vds.id
                                         ,wim.site=wim.site
                                         ,direction=direction
                                         ,year=year)
        expect_true(have_one)

        ## getting the output should be the same as input
        duplicate.df <- couch.get.merged.pair(parts,vds.id,wim.site,direction,year)

        ## print(summary(duplicate.df))
        expect_that(df.merged,equals(duplicate.df))

    })

unlink(filepath[1])
rcouchutils::couch.deletedb(parts)
