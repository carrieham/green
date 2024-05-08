#####cran: https://cran.r-project.org/web/packages/congress/congress.pdf
#####NOTE: 
#####INSTALL PACKAGES
options(repos = list(CRAN="http://cran.rstudio.com/"))

install.packages("pacman")
library(pacman)
p_load(tidyverse, congress, httr, federalregister, readxl, xml2, sparklyr)

rm(list=ls())


#Establish input/output drive shortcut
myDrive2="data/hein-bound/"

#input files to R
bound_speechesFiles <- list.files(path=paste(myDrive2,"speeches",sep=""),full.names=TRUE)
bound_descrFiles <- list.files(path=paste(myDrive2,"descr",sep=""),full.names=TRUE)
# byspeakerFiles <- list.files(path=paste(myDrive,"byspeaker",sep=""),full.names=TRUE)
# bypartyFiles <- list.files(path=paste(myDrive,"byparty",sep=""),full.names=TRUE)
# SpeakerMapFiles <- list.files(path=paste(myDrive,"SpeakerMap",sep=""),full.names=TRUE)

#merge files
bound_speeches <- bound_speechesFiles %>%
  map_dfr(read_delim)

bound_descr <- bound_descrFiles %>%
  map_dfr(read_delim)

head(bound_speeches)
head(bound_descr)