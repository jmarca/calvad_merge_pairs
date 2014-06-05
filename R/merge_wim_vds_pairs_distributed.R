library('R.utils')
library('RPostgreSQL')
m <- dbDriver("PostgreSQL")

## requires environment variables be set externally
psqlenv = Sys.getenv(c("PSQL_HOST", "PSQL_USER", "PSQL_PASS"))

con <-  dbConnect(m
                  ,user=psqlenv[2]
                  ,password=psqlenv[3]
                  ,host=psqlenv[1]
                  ,dbname="spatialvds")

source('../components/jmarca-rstats_couch_utils/couchUtils.R',chdir=TRUE)
source('../components/jmarca-calvad_rscripts/lib/process.wim.site.R',chdir=TRUE)
source('get.wim.R')


couch.set.wim.paired.state <- function(year,wim.site,direction,state,local=TRUE){
  couch.set.state(year=year,detector.id=paste('wim',wim.site,direction,sep='.'),doc=list('paired'=state),local=local)
}


## for plotting scatter plots
pf <- function(x,y){panel.smoothScatter(x,y,nbin=c(200,200))}
day.of.week <- c('Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday')
lane.defs <- c('left lane','right lane 1', 'right lane 2', 'right lane 3', 'right lane 4', 'right lane 5', 'right lane 6', 'right lane 7', 'right lane 8')


make.merged.filepath <- function(vdsid,year,wim.id,direction){
  savepath <- paste(wim.path,year,sep='/')
  if(!file.exists(savepath)){dir.create(savepath)}
  savepath <- paste(savepath,wim.id,sep='/')
  if(!file.exists(savepath)){dir.create(savepath)}
  savepath <- paste(savepath,direction,sep='/')
  if(!file.exists(savepath)){dir.create(savepath)}
  filename <- paste('wim',wim.id,direction,'vdsid',vdsid,year,'paired','RData',sep='.')
  filepath <- paste(savepath,filename,sep='/')
  return (c(filepath,filename))
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
if(is.null(wim.path)){
  print('assign a valid direectory to the WIM_PATH environment variable')
  quit('no',1)
}
print(paste('wim.path <-',wim.path))


year <- as.numeric(Sys.getenv(c('RYEAR'))[1])
if(is.null(year)){
  print('assign the year to process to the RYEAR environment variable')
  quit('no',1)
}
print(paste('year <-',year))



wim.site <- Sys.getenv(c('WIM_SITE'))[1]
if(is.null(wim.site)){
  print('assign a valid site to the WIM_SITE environment variable')
  quit('no',1)
}
print(paste('wim.site <-',wim.site))

direction <- Sys.getenv(c('WIM_DIRECTION'))[1]
if(is.null(direction)){
  print('assign a valid direction to the WIM_DIRECTION environment variable')
  quit('no',1)
}
print(paste('direction <-',direction))

## convenience...the canonical way to id in couchdb
cdb.wimid <- paste('wim',wim.site,direction,sep='.')


## get wim file
df.wim.zoo <-get.wim.imputed(wim.site,year,direction,wim.path=wim.path)
print(paste('got wim zoo okay, length',length(df.wim.zoo)))

if( length(df.wim.zoo) == 1 ){
    print(paste("amelia run for wim not good",df.wim.zoo))
    quit('no',1)
}

print('going to fetch nearby vds')

## combine WIM imputation with paired VDS imputation


## paired.vds <- wim.vds.pairs[wim.vds.pairs$wim_id==wim.site & wim.vds.pairs$dir==direction,]

nearby.vds <- get.list.regenerate.wim.pairs(wim.site
                                            ,direction
                                            ,samefreeway=TRUE)

print('got nearby vds')
print(nearby.vds)
## don't do this for pairs
## if(dim(nearby.vds)[1]==0){
##     nearby.vds  <- get.list.regenerate.wim.pairs(wim.site
##                                             ,direction
##                                             ,samefreeway=FALSE)
## }

## limit to 1.6km (1 mile) away for now
nearby.vds <- nearby.vds[nearby.vds$distance<=1600,]
print('limited')
print(nearby.vds)

## get "paired" by just pulling some, and looping till we get something good
gotgoodpair <- FALSE
pairidx <- 1

neighborslength <- dim(nearby.vds)[1]
if(neighborslength==0){
    couch.set.state(year=year,detector.id=cdb.wimid,doc=list('paired'='none'
                                                        ,'neighbors'='none'))
    gotgoodpair=TRUE
    q("no", 10, FALSE)
}


nearby.vds$dir <- capitalize(substr(nearby.vds$direction,1,1))
merged.vds <- data.frame()

print('looping to load a good vds site and pair it')
while(!gotgoodpair && pairidx <= neighborslength ){
    paired.vds <- nearby.vds[pairidx,]
    pairidx <- pairidx+1
    for(pair in paired.vds$vds_id){
        ## check if a merged file is already attached to doc. if so,
        ## then bail
        filepath <- make.merged.filepath(pair,year,wim.site,direction)
        if(couch.has.attachment(docname=pair
                                ,attachment=filepath[2])
           ) {
            merged.vds[dim(merged.vds)[1]+1,'merged'] <- pair
            next
        }

        df.vds.zoo <- get.and.plot.vds.amelia(pair,year,cdb.wimid,doplots=doplots,remote=FALSE,path='/data/backup/pems')
        if(is.null(df.vds.zoo) ){
            next
        }
        ## combine the vds zoo with the wim zoo
        df.merged <- merge(df.vds.zoo,df.wim.zoo,all=TRUE,suffixes = c("vds","wim"))
        ts.ts <- unclass(time(df.merged))+ISOdatetime(1970,1,1,0,0,0,tz='UTC')
        keep.columns <-  grep( pattern="(^ts|^day|^tod|^obs_count)",x=names(df.merged),perl=TRUE,value=TRUE,invert=TRUE)
        df.merged <- data.frame(coredata(df.merged[,keep.columns]))
        df.merged$ts <- ts.ts
        ts.lt <- as.POSIXlt(df.merged$ts)
        df.merged$tod   <- ts.lt$hour + (ts.lt$min/60)
        df.merged$day   <- ts.lt$wday

        save(df.merged,file=filepath[1],compress='xz')
        couch.attach('vdsdata%2Ftracking',pair,filepath[1], local=TRUE)
        merged.vds[dim(merged.vds)[1]+1,'merged'] <- pair
        rm(df.merged,df.vds.zoo)
        ## gc()
    }
    if(dim(merged.vds)[1]>0){
        couch.set.state(year=year
                        ,detector.id=cdb.wimid
                        ,doc=list('merged'=merged.vds$merged))
        gotgoodpair <- TRUE
    }
}

if(dim(merged.vds)[1]==0){
    couch.set.state(year=year
                    ,detector.id=cdb.wimid
                    ,doc=list('merged'='nopair',
                              'neighbors'=neighborslength))
}
