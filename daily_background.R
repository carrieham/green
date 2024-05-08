#####cran: https://cran.r-project.org/web/packages/congress/congress.pdf
#####NOTE: 
#####INSTALL PACKAGES
options(repos = list(CRAN="http://cran.rstudio.com/"))

install.packages("pacman")
library(pacman)
p_load(tidyverse, congress, httr, federalregister, readxl, xml2, sparklyr)

rm(list=ls())

# ARCHIVE -------------------------------------------------
# #congressional dataset
# congress <- read_delim("data/hein-daily/speeches_107.txt")
# 
# #Establish input/output drive shortcut
# myDrive="data/hein-daily/"
# 
# #input files to R
# daily_speechesFiles <- list.files(path=paste(myDrive,"speeches",sep=""),full.names=TRUE)
# daily_descrFiles <- list.files(path=paste(myDrive,"descr",sep=""),full.names=TRUE)
# # byspeakerFiles <- list.files(path=paste(myDrive,"byspeaker",sep=""),full.names=TRUE)
# # bypartyFiles <- list.files(path=paste(myDrive,"byparty",sep=""),full.names=TRUE)
# # SpeakerMapFiles <- list.files(path=paste(myDrive,"SpeakerMap",sep=""),full.names=TRUE)
# 
# #merge files
# daily_speeches <- daily_speechesFiles %>%
#   map_dfr(read_delim)
# 
# daily_descr <- daily_descrFiles %>%
#   map_dfr(read_delim)
# 
# head(daily_speeches)
# head(daily_descr)
# 
# # byspeaker <- byspeakerFiles %>%
# #   map_dfr(read_delim)
# # head(byspeaker)
# # 
# # byparty <- bypartyFiles %>%
# #   map_dfr(read_delim)
# # head(byparty)
# 
# # SpeakerMap <- SpeakerMapFiles %>%
# #   map_dfr(read_delim)
# # head(SpeakerMap)
# 
# #merge speeches with metadata
# daily_diff <- setdiff(descr$speech_id,speeches$speech_id)
# daily_diff2 <- descr %>%
#   filter(speech_id %in% diff)
# daily_diffDates <- unique(diff2$date)
# 
# #congress <- bind_cols(speeches,descr)
# daily_congress <- merge(speeches,descr)


# DAILY -------------------------------------------------
#Establish input/output drive shortcut
myDrive="data/hein-daily/"

#input files to R
daily_speechesFiles <- list.files(path=paste(myDrive,"speeches",sep=""),full.names=TRUE)
daily_descrFiles <- list.files(path=paste(myDrive,"descr",sep=""),full.names=TRUE)

spark_install(version = "3.4")

sc <- spark_connect(master = "local")

daily_descr <- spark_read_csv(sc, daily_descrFiles[1],
                              name="daily_descr",
                              delimiter="|",
                              overwrite=TRUE,
                              header=TRUE,
                              repartition=0) 
counter=1
for (aFile in daily_descrFiles[2:length(daily_descrFiles)]) {
  counter = counter +1 
  daily_descr <- sdf_bind_rows(daily_descr,
                               spark_read_csv(sc,aFile,
                                              name=paste("daily_descr",counter, sep="_"),
                                              delimiter="|",
                                              overwrite=TRUE,
                                              header=TRUE,
                                              repartition=0))
}



daily_speeches <- spark_read_csv(sc, daily_speechesFiles[1],
                                 name="daily_speeches",
                                 delimiter="|",
                                 overwrite=TRUE,
                                 header=TRUE,
                                 repartition=0) 
counter=1
for (aFile in daily_speechesFiles[2:length(daily_speechesFiles)]) {
  counter = counter +1 
  daily_speeches <- sdf_bind_rows(daily_speeches,
                                  spark_read_csv(sc,aFile,
                                                 name=paste("daily_speeches",counter, sep="_"),
                                                 delimiter="|",
                                                 overwrite=TRUE,
                                                 header=TRUE,
                                                 repartition=0))
}


daily_full <- daily_speeches %>%
  left_join(daily_descr)