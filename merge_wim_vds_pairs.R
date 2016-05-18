## need node_modules directories
dot_is <- getwd()
node_paths <- dir(dot_is,pattern='\\.Rlibs',
                  full.names=TRUE,recursive=TRUE,
                  ignore.case=TRUE,include.dirs=TRUE,
                  all.files = TRUE)
path <- normalizePath(node_paths, winslash = "/", mustWork = FALSE)
lib_paths <- .libPaths()
.libPaths(c(path, lib_paths))
print(path)
print(.libPaths())

pkg <- devtools::as.package('.')
ns_env <- devtools::load_all(pkg,quiet = TRUE)$env

## need env for test file
config_file <- Sys.getenv('R_CONFIG')

if(config_file ==  ''){
    config_file <- 'config.json'
}
print(paste ('using config file =',config_file))
config <- rcouchutils::get.config(config_file)
db <- config$couchdb$trackingdb

redo <- config$calvad$redo_pairs
if(is.null(redo) || '' == redo){
    redo <- FALSE
}
print(paste('redo is set to ',redo))

## which VDS site or sites?
##wim.vds.pairs <- get.list.closest.wim.pairs()
##wim.vds.pairs$dir <- capitalize(substr(wim.vds.pairs$direction,1,1))

###########################
## process the specified wim site
###########################

seconds <- 3600

doplots <- FALSE

wim.path <- Sys.getenv(c('WIM_PATH'))[1]
if(is.null(wim.path) || '' == wim.path){
    ## try the config file
    if(! is.null(config$calvad)){
        if(! is.null(config$calvad$wimpath)){
            wim.path <- config$calvad$wimpath

        }
    }
}
if(is.null(wim.path) || '' == wim.path){

    print('assign a valid direectory to the WIM_PATH environment variable')
    quit('no',1)
}

print(paste('wim.path <-',wim.path))



vds.path <- Sys.getenv(c('VDS_PATH'))[1]
if(is.null(vds.path) || '' == vds.path){
    ## try the config file
    if(! is.null(config$calvad)){
        if(! is.null(config$calvad$vdspath)){
            vds.path <- config$calvad$vdspath

        }
    }
}
if(is.null(vds.path) || '' == vds.path){

    print('assign a valid direectory to the VDS_PATH environment variable')
    quit('no',1)
}

print(paste('vds.path <-',vds.path))

year <- as.numeric(Sys.getenv(c('RYEAR'))[1])
if(is.null(year)){
  print('assign the year to process to the RYEAR environment variable')
  quit('no',1)
}
print(paste('year <-',year))



wim.site <- Sys.getenv(c('WIM_SITE'))[1]
if('' == wim.site || is.null(wim.site)){
  print('assign a valid site to the WIM_SITE environment variable')
  quit('no',1)
}
print(paste('wim.site <-',wim.site))

direction <- Sys.getenv(c('WIM_DIRECTION'))[1]
if('' == direction || is.null(direction)){
  print('assign a valid direction to the WIM_DIRECTION environment variable')
  quit('no',1)
}
print(paste('direction <-',direction))

## convenience...the canonical way to id in couchdb
cdb.wimid <- paste('wim',wim.site,direction,sep='.')

vds.ids <- Sys.getenv(c('VDS_IDS'))[1]
if('' == vds.ids || is.null(vds.ids)){
  print('assign a valid id to the VDS_IDS environment variable')
  quit('no',1)
}

vds.ids <- c(strsplit(x=vds.ids,split=','))
vds.ids <- matrix(data=unlist(vds.ids)
                 ,ncol=2,byrow=TRUE)

vds.ids <- data.frame('vdsid'=vds.ids[,1],
                      'dist'=as.numeric(vds.ids[,2])
                      )

## get wim file

df.wim.imputed <- calvadrscripts::get.amelia.wim.file.local(site_no=wim.site
                                           ,year=year
                                           ,direction=direction
                                           ,path=paste(wim.path,year,sep='/'))

if( length(df.wim.imputed) == 1 ){
    print(paste("amelia run for wim not good",df.wim.imputed))
    quit('no',1)
}

df.wim.merged <- calvadrscripts::condense.amelia.output(df.wim.imputed)


## get "paired" by stepping down the vdsids list until we get
## something good

gotgoodpair <- 0

## target pair is how many pairs you want to generate.  sometimes
## there is an argument to have more than one, say if there is
## excessive distance between the WIM site and the nearest VDS
## will default to 1
targetpair <- Sys.getenv(c('CALVAD_TARGET_PAIR'))[1]
if('' == targetpair || is.null(targetpair)){
    targetpair <- 1
}else{
    targetpair <- as.numeric(targetpair)
}
print(paste('targetpair <-',targetpair))


pairidx <- 1
neighborslength <- length(vds.ids[,1])
if(neighborslength==0){
    rcouchutils::couch.set.state(year=year,
                                 id=cdb.wimid,
                                 doc=list(
                                     'paired'='none'
                                    ,'neighbors'='none'),
                                 db=db)
    q("no", 10, FALSE)
}


merged.vds <- data.frame()

print('looping to load a good vds site and pair it')
while(length(merged.vds) < targetpair && pairidx <= neighborslength ){
    vds.id <- vds.ids[pairidx,1]
    print(paste('vds.id <-',vds.id))
    pairidx <- pairidx+1
        ## check if a merged file is already attached to doc. if so,
        ## then bail
    have_one <- couch.has.merged.pair(trackingdb=db
                                     ,vds.id=vds.id
                                     ,wim.site=wim.site
                                     ,direction=direction
                                     ,year=year)
    if(have_one && !redo) {
        print(paste('already paired',have_one,redo))
        merged.vds[dim(merged.vds)[1]+1,'merged'] <- vds.id
        next
    }
    df.merged <- merge_wim_with_vds(df.wim.merged=df.wim.merged
                                ,wim.site=wim.site
                                ,direction=direction
                                ,vds.id=vds.id
                                ,path=vds.path
                                ,year=year
                                ,trackingdb=db
                                 )

    print(paste("dim(df.wim.merged) <-", paste(dim(df.wim.merged),collapse=',')))
    print(paste("dim(df.merged) <-", paste(dim(df.merged),collapse=',')))
    if(dim(df.merged)[1] == 0){
        print("no overlap, try the next one")
        next
    }

    all_the_hours <- plyr::count(df.merged$ts)
    for(i in 1:length(all_the_hours)){
        testthat::expect_that(all_the_hours[i,2],testthat::equals(1))
    }

    ## save to filesystem for great justice and backups
    filepath <- make.merged.filepath(vds.id,year,wim.site,direction,wim.path)
    save(df.merged,file=filepath[1],compress='xz')

    ## attach to couchdb for convenience of use
    couch.put.merged.pair(trackingdb=db,
                          vds.id=vds.id,
                          file=filepath[1])

    merged.vds[dim(merged.vds)[1]+1,'merged'] <- vds.id

}

if(dim(merged.vds)[1]>0){
    res <- rcouchutils::couch.set.state(year=year,
                                        id=cdb.wimid,
                                        doc=list('merged'=merged.vds$merged),
                                        db=db)

}else{
    res <- rcouchutils::couch.set.state(year=year,
                                        id=cdb.wimid,
                                        doc=list('merged'='nopair',
                                            'neighbors'=neighborslength),
                                        db=db)
}

quit('no',10)
