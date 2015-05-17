## need node_modules directories
setwd('..')
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

## need env for test file
config_file <- Sys.getenv('R_CONFIG')

if(config_file ==  ''){
    config_file <- 'config.json'
}
print(paste ('using config file =',config_file))
config <- rcouchutils::get.config(config_file)
db <- config$couchdb$trackingdb

## pass it the raw data details, and either the raw data will get
## loaded and parsed and saved as a dataframe, or else the existing
## dataframe will get loaded.  In either case, the plots will get made
## and saved to couchdb


library('RPostgreSQL')
m <- dbDriver("PostgreSQL")
con <-  dbConnect(m
                  ,user=config$postgresql$auth$username
                  ,host=config$postgresql$host
                  ,dbname=config$postgresql$db)


couch.set.wim.paired.state <- function(year,wim.site,direction,state,local=TRUE){
    couch.set.state(year=year,
                    detector.id=paste('wim',wim.site,direction,sep='.'),
                    doc=list('paired'=state),
                    db=db)
}



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
                                           ,path=wim.path)

if( length(df.wim.imputed) == 1 ){
    print(paste("amelia run for wim not good",df.wim.imputed))
    quit('no',1)
}

df.wim.merged <- calvadrscripts::condense.amelia.output(df.wim.imputed)

print(summary(df.wim.merged))
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
    couch.set.state(year=year,detector.id=cdb.wimid,
                    doc=list('paired'='none'
                       ,'neighbors'='none'))
    q("no", 10, FALSE)
}


merged.vds <- data.frame()

print('looping to load a good vds site and pair it')
while(gotgoodpair < targetpair && pairidx <= neighborslength ){
    vds.id <- vds.ids[pairidx,1]
    pairidx <- pairidx+1
        ## check if a merged file is already attached to doc. if so,
        ## then bail
    filepath <- make.merged.filepath(vds.id,year,wim.site,direction)
    print(filepath)
    have_one <- rcouchutils::couch.has.attachment(db=db
                                                 ,docname=vds.id
                                                 ,attachment=filepath[2])
    if(have_one) {
        merged.vds[dim(merged.vds)[1]+1,'merged'] <- vds.id
        gotgoodpair <- gotgoodpair + 1
        next
    }

    print(paste('loading',vds.id,'from',vds.path))
    df.vds.merged <- calvadrscripts::get.and.plot.vds.amelia(
        pair=vds.id,
        year=year,
        doplots=FALSE,
        remote=FALSE,
        path=vds.path,
        force.plot=FALSE,
        trackingdb=db)


    print(summary(df.vds.merged))

    df.merged <- merge(df.vds.merged, df.wim.merged,
                       all=FALSE,
                       suffixes = c("vds","wim"))

    ## entering all=FALSE above truncates the non-overlap

    ## save to filesystem for great justice and backups
    save(df.merged,file=filepath[1],compress='xz')

    ## attach to couchdb for convenience of use
    rcouchutils::couch.attach(db,vds.id,filepath[1], local=TRUE)

    ## record keeping of successful merged pairs
    merged.vds[dim(merged.vds)[1]+1,'merged'] <- vds.id
    rm(df.merged,df.vds.merged)

}

if(dim(merged.vds)[1]>0){
    couch.set.state(year=year
                   ,detector.id=cdb.wimid
                   ,doc=list('merged'=merged.vds$merged))

}else{
    couch.set.state(year=year
                   ,detector.id=cdb.wimid
                   ,doc=list('merged'='nopair',
                        'neighbors'=neighborslength))
}
