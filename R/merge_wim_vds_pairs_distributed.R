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
wim.vds.pairs <- get.list.closest.wim.pairs()
wim.vds.pairs$dir <- capitalize(substr(wim.vds.pairs$direction,1,1))

###########################
## process the specified wim site
###########################

wim.path <- Sys.getenv(c('WIM_PATH'))[1]
if(is.null(wim.path)){
  print('assign a valid direectory to the WIM_PATH environment variable')
  exit(1)
}

doplots = FALSE

year <- as.numeric(Sys.getenv(c('RYEAR'))[1])
if(is.null(year)){
  print('assign the year to process to the RYEAR environment variable')
  exit(1)
}
print(year)

seconds <- 3600

wim.site <- Sys.getenv(c('WIM_SITE'))[1]
if(is.null(wim.site)){
  print('assign a valid site to the WIM_SITE environment variable')
  exit(1)
}

direction <- Sys.getenv(c('WIM_DIRECTION'))[1]
if(is.null(direction)){
  print('assign a valid direction to the WIM_DIRECTION environment variable')
  exit(1)
}

## convenience...the canonical way to id in couchdb
cdb.wimid <- paste('wim',wim.site,direction,sep='.')


## get wim file
df.wim.zoo <-get.wim.imputed(wim.site,year,direction,wim.path=wim.path)
if(length(df.wim.zoo) == 1 || df.wim.zoo == 1){
    print(paste("amelia run for wim not good",df.wim.zoo))
    exit(1)
}

## combine WIM imputation with paired VDS imputation


paired.vds <- wim.vds.pairs[wim.vds.pairs$wim_id==wim.site & wim.vds.pairs$dir==direction,]
if(dim(paired.vds)[1]==0){
    couch.set.state(year=year,detector.id=cdb.wimid,doc=list('paired'='none'))
}else{
    couch.set.state(year=year
                    ,detector.id=cdb.wimid
                    ,doc=list('paired'=paired.vds$vds_id))
}
for(pair in paired.vds$vds_id){
    ## check if a merged file is already attached to doc. if so, then bail
    filepath <- make.merged.filepath(pair,year,wim.site,direction)
    if(couch.has.attachment(docname=pair
                            ,attachment=filepath[2])
       ) {
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
    rm(df.merged,df.vds.zoo)
    ## gc()
}
exit('done')
