# Merge Pairs

If I want to keep my projects from swelling up with bloat, I have to
make a lot of them.

This repo is just for code that merges paired WIM and VDS sites.  It
is a refactor of code from my bdp repo, and will rely heavily on my
calvad_rscripts, etc components.

# Refactor and rewrite

Rather than leaving most of the work in calvad_data_extract sql files,
and recognizing that the pairing and merging step has to happen after
both VDS and WIM have been imputed, I am rewriting this to establish
the pairing between the two, and also to actually merge the data.

Connecting a WIM site and a VDS detector is only relevant if the
detectors have valid data.

So the first step is to hit the couchdb tracking db and get a list of
WIM sites with valid data for the given year.

Then examine the VDS to WIM distance table to pick off the closest VDS
sites to each WIM site with valid data.  Working down the list in
order of distance, and keeping in mind there might be ties or near
neighbors, *and* there might be periods in the year with not enough
data to cover the year necessitating more than one pairing, check each
VDS against couchdb records of imputation result.  If imputation good,
then do the pairing and save it.

The question is whether to put it in R or node.js.  The merging
happens in R, but it is probably best to do the checking in node.js.
This allows me to create the distance table as a temp table, oneoff
for the year, with just the WIM sites I care about, and then use it
repeatedly with the same db connection.  In contrast each R instance
will have a separate DB connection.

# Neighbor links

There is a more complicated issue with neighbors.  The problem is that
they don't line up on same freeways, same direction.  So that query
will have to be without freeway and direction limits, but perhaps with
some sort of spatial inclusion...up down left right.

Anyway, neighbors are handled in the repository calvad_wim_neighbors
