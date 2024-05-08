#####cran: https://cran.r-project.org/web/packages/congress/congress.pdf
#####NOTE: 
#####INSTALL PACKAGES
options(repos = list(CRAN="http://cran.rstudio.com/"))

install.packages("pacman")
library(pacman)
p_load(tidyverse, congress, httr, federalregister, readxl, xml2, sparklyr)

rm(list=ls())

# ARCHIVE -------------------------------------------------
# #Establish input/output drive shortcut
# myDrive2="data/hein-bound/"
# 
# #input files to R
# bound_speechesFiles <- list.files(path=paste(myDrive2,"speeches",sep=""),full.names=TRUE)
# bound_descrFiles <- list.files(path=paste(myDrive2,"descr",sep=""),full.names=TRUE)
# # byspeakerFiles <- list.files(path=paste(myDrive,"byspeaker",sep=""),full.names=TRUE)
# # bypartyFiles <- list.files(path=paste(myDrive,"byparty",sep=""),full.names=TRUE)
# # SpeakerMapFiles <- list.files(path=paste(myDrive,"SpeakerMap",sep=""),full.names=TRUE)
# 
# # #merge files
# # bound_speeches <- bound_speechesFiles %>%
# #   map_dfr(read_delim)
# # 
# # bound_descr <- bound_descrFiles %>%
# #   map_dfr(read_delim)
# # 
# # head(bound_speeches)
# # head(bound_descr)

# bound_speeches <- map_dfr(spark_read, sc, path=bound_speechesFiles, overwrite=FALSE)
# 
# bound_speeches <- spark_read(sc, paths=bound_speechesFiles)
# 
# # bound_speeches <- bound_speechesFiles %>%
# #   spark_read(header=TRUE, memory=FALSE, overwrite=FALSE)
# 
# bound_speeches <- bound_speechesFiles %>%
#   map_dfr(spark_read(header=TRUE, memory=FALSE, overwrite=FALSE))
# 
# 
# spark_read_text(sc,name="review_tbl",
#                               path="yelp_dataset/review.json", 
#                               header = TRUE, 
#                               memory = FALSE,
#                               overwrite = TRUE)
# 

# bound_descr <- spark_read_csv(sc, bound_descrFiles[1],
#                               name="bound_descr",
#                               delimiter="|",
#                               overwrite=TRUE,
#                               header=TRUE,
#                               repartition=0)
# for (aFile in bound_descrFiles[2:length(bound_descrFiles)]) {
#   bound_descr <- rbind(bound_descr,
#                           spark_read_csv(sc, aFile,
#                                          name="bound_descr",
#                                          delimiter="|",
#                                          overwrite=FALSE,
#                                          header=TRUE,
#                                          repartition=0))
# }

# 
# bound_speeches <- spark_read_csv(sc, bound_speechesFiles[1],
#                                  name="bound_speeches",
#                                  delimiter="|",
#                                  overwrite=TRUE,
#                                  header=TRUE)
# for (aFile in bound_speechesFiles[2:length(bound_speechesFiles)]) {
#   bound_speeches <- rbind(bound_speeches,
#                           spark_read_csv(sc, aFile,
#                                          name="bound_speeches",
#                                          delimiter="|",
#                                          overwrite=FALSE,
#                                          header=TRUE))
# }


# BOUND -------------------------------------------------
#Establish input/output drive shortcut
myDrive2="data/hein-bound/"

#input files to R
bound_speechesFiles <- list.files(path=paste(myDrive2,"speeches",sep=""),full.names=TRUE)
bound_descrFiles <- list.files(path=paste(myDrive2,"descr",sep=""),full.names=TRUE)

spark_install(version = "3.4")
sc <- spark_connect(master = "local")

bound_descr <- spark_read_csv(sc, bound_descrFiles[1],
                              name="bound_descr",
                              delimiter="|",
                              overwrite=TRUE,
                              header=TRUE,
                              repartition=0) 
counter=1
for (aFile in bound_descrFiles[2:length(bound_descrFiles)]) {
  counter = counter +1 
  bound_descr <- sdf_bind_rows(bound_descr,
                               spark_read_csv(sc,aFile,
                                              name=paste("bound_descr",counter, sep="_"),
                                              delimiter="|",
                                              overwrite=TRUE,
                                              header=TRUE,
                                              repartition=0))
}
  
  
  
  bound_speeches <- spark_read_csv(sc, bound_speechesFiles[1],
                                   name="bound_speeches",
                                   delimiter="|",
                                   overwrite=TRUE,
                                   header=TRUE,
                                   repartition=0) 
  counter=1
  for (aFile in bound_speechesFiles[2:length(bound_speechesFiles)]) {
    counter = counter +1 
    bound_speeches <- sdf_bind_rows(bound_speeches,
                                    spark_read_csv(sc,aFile,
                                                   name=paste("bound_speeches",counter, sep="_"),
                                                   delimiter="|",
                                                   overwrite=TRUE,
                                                   header=TRUE,
                                                   repartition=0))
  }
  
  
  bound_full <- bound_speeches %>%
    left_join(bound_descr)