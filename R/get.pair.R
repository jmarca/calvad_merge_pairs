## get or create a paired VDS site for a WIM site

## I have a table for this, BUT, the problem is that the paired sites
## have data in some years, no data in others.  To date I've been
## simply adding more detectors in each subsequent year of analysis.
## This is pretty ad hoc.  Better (still ad hoc, however) is to look
## at the neighbors list fresh each time, and determine the closest
## VDS, then work down the line until I find one that is close and has
## data.

## two cases

## first, hit sql query, get closest 10, say.  Rinse repeat until I
## find something good

## second, if nothing with standard sql on freeway, then need neighboring freeway?

## for the second case, perhaps look at what is already in the pairs table?



## select b.vds_id,b.site_no,b.freeway,b.direction,b.dist
## from  imputed.vds_wim_neighbors b
## join wim_freeway wf on (wf.wim_id=b.site_no and wf.freeway_id=b.freeway)
## where b.site_no=
##     and b.direction=
##     order by dist
## limit 50;

## stuck this code inside wim.loading.functions.R
