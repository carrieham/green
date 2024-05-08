#####cran: https://cran.r-project.org/web/packages/congress/congress.pdf
#####NOTE: 
options(repos = list(CRAN="http://cran.rstudio.com/"))
#####INSTALL PACKAGES
install.packages("pacman")
library(pacman)
p_load(tidyverse, tidytext, congress, httr, federalregister, readxl, xml2, sparklyr, tictoc)

rm(list=ls())

spark_install(version = "3.4")
config <- spark_config()
config["sparklyr.shell.driver-memory"] <- "16G"
config["sparklyr.shell.executor-memory"] <- "16G"
config$spark.yarn.executor.memoryOverhead <- "10g"
config$spark.driver.maxResultSize <- "3g"
sc <- spark_connect(master = "local", config = config)

#host <- "hb.ucsc.edu (169.233.5.132)"
#sc <- spark_connect(master = "yarn-cluster")

# OTHER PACKAGES/COMMANDS  -------------------------------------------------
# cong_committee_report() #committee reports
# cong_committee_print() #committee prints 
# cong_committee_meeting() #committee meetings
# cong_record() #Congressional Records
# cong_daily_record() #Daily Congressional Records
# cong_bound_record() #Bound Congressional Records
# cong_house_communication() #House communications
# cong_senate_communication() #Senate communications

# ARCHIVE  -------------------------------------------------
#####TEST
# test <- cong_daily_record(
#   volume=167,
#   issue=21,
#   item="articles"
# )

# text <- content(GET("https://www.congress.gov/117/crec/2021/02/04/167/21/modified/CREC-2021-02-04-pt1-PgD93-2.htm"), encoding = "UTF-8")
# cat(substring(text, 1, 1000))
# 
# arecord <- fr_get('E9-1719')
# full <- httr::content(httr::GET(arecord[[1]]$raw_text_url), "text", )
# test2<- fr_search(term="energy crisis",order="oldest") %>%
#   content(GET(),"text")

# DICTIONARIES
default_stop_words <- ml_default_stop_words(spark_connection(sc), "english") 

manual_stop_words <- c("absent","adjourn", "ask", "can", "chairman",
                       "committee","con","democrat","etc","gentleladies",
                       "gentlelady", "gentleman", "gentlemen", "gentlewoman",
                       "gentlewomen","hereabout","hereafter","hereat", "hereby",
                       "herein", "hereinafter", "hereinbefore", "hereinto", "hereof",
                       "hereon", "hereto", "heretofore", "hereunder", "hereunto",
                       "hereupon", "herewith", "month", "mr", "mrs", "nai", "nay",
                       "none", "now", "part", "per", "pro", "republican",
                       "say", "senator", "shall", "sir", "speak", "speaker",
                       "tell", "thank", "thereabout", "thereafter", "thereagainst",
                       "thereat", "therebefore", "therebeforn", "thereby", "therefor",
                       "therefore", "therefrom", "therein", "thereinafter", "thereof",
                       "thereon", "thereto", "theretofore", "thereunder", "thereunto",
                       "thereupon", "therewith", "therewithal", "today", "whereabouts",
                       "whereafter", "whereas", "whereat", "whereby","wherefore",
                       "wherefrom", "wherein", "whereinto", "whereof", "whereon","whereto",
                       "whereunder", "whereupon", "wherever", "wherewith", 
                       "wherewithal", "will", "yea", "yes", "yield")

stop <- c(manual_stop_words,default_stop_words)

enviroVocab <- c("climate change",
                 "sustainability",
                 "global warming",
                 "microplastic",
                 "emissions",
                 "carbon",
                 "climate strikes",
                 "extreme weather",
                 "greenhouse effect",
                 "global warming",
                 "climate crisis",
                 "climate emergency",
                 "global heating",
                 "climate refugee",
                 "eco-anxiety",
                 "food insecurity",
                 "water insecurity",
                 "ecocide",
                 "overconsumption",
                 "unsustainable",
                 "degrowth",
                 "decoupling",
                 "single-use",
                 "CO2",
                 "NOX",
                 "H2O",
                 "flood",
                 "wildfire",
                 "superstorm",
                 "extreme weather",
                 "rain garden",
                 "urban agriculture",
                 "vertical farming",
                 "food miles",
                 "carbon capture",
                 "air-source",
                 "ground-source",
                 "windmill",
                 "microgrid",
                 "retrofit",
                 "smart charging",
                 "range anxiety",
                 "active travel",
                 "carbon footprint",
                 "natural capital",
                 "ecosystem services",
                 "kaitiakitanga",
                 "tipping point",
                 "mass extinction")

# DAILY -------------------------------------------------
#Establish input/output drive shortcut
myDrive="data/hein-daily/"

#input files to R
daily_speechesFiles <- list.files(path=paste(myDrive,"speeches",sep=""),full.names=TRUE)
daily_descrFiles <- list.files(path=paste(myDrive,"descr",sep=""),full.names=TRUE)

#DAILY DESC
daily_descr <- spark_read_csv(
  path = "data/hein-daily/descr/",
  sc = sc, 
  name = "daily_descr_tbl", 
  delimiter = "|", 
  overwrite=TRUE,
  header = TRUE,
  memory = FALSE)

#DAILY SPEECHES
daily_speeches <- spark_read_csv(
  path = "data/hein-daily/speeches/",
  sc = sc, 
  name = "daily_speeches_tbl", 
  delimiter = "|", 
  overwrite=TRUE,
  header = TRUE,
  memory=FALSE) 

#CHECKING
# pull(daily_descr,date) %>% max()
# pull(daily_speeches,speech_id) %>% max()

daily_full <- daily_speeches %>%
  left_join(daily_descr) %>%
  filter(year>=2010)

#find minimum date to filter bound data
#pull(daily_full,date) %>% min()

# BOUND -------------------------------------------------
#Establish input/output drive shortcut
myDrive2="data/hein-bound/"

#input files to R
bound_speechesFiles <- list.files(path=paste(myDrive2,"speeches",sep=""),full.names=TRUE)
bound_descrFiles <- list.files(path=paste(myDrive2,"descr",sep=""),full.names=TRUE)

#BOUND DESCR
bound_descr <- spark_read_csv(
  path = "data/hein-bound/descr/",
  sc = sc, 
  name = "bound_descr_tbl", 
  delimiter = "|", 
  overwrite=TRUE,
  header = TRUE,
  memory=FALSE)

#BOUND SPEECHES
bound_speeches <- spark_read_csv(
  path = "data/hein-bound/speeches/",
  sc = sc, 
  name = "bound_speeches_tbl", 
  delimiter = "|", 
  overwrite=TRUE,
  header = TRUE,
  memory=FALSE)

#CHECKING
# pull(bound_descr,date) %>% max()
# pull(bound_speeches,speech_id) %>% max()

#COMBINE DESC + SPEECHES
bound_full <- bound_speeches %>%
  left_join(bound_descr) %>%
  filter(date<19810105) %>%
  filter(year>=2010)

#verify filtering
#pull(bound_full,date) %>% max()

# MERGE & TOKENIZE -------------------------------------------------
full <- sdf_bind_rows(daily_full,bound_full) %>%
  mutate(year=substr(date,1,4)) %>%
  filter(year==2010)%>%
  #compute("full_tbl")
  sdf_register()
# 
# full %>%
#   tally()

#tbl_cache(sc, "full_tbl")

full_tokens <- full %>%
  mutate(speech = regexp_replace(speech, 
                                 "[_\"\'():;,.!?\\-]", 
                                 " ")) %>%
  mutate(speech = regexp_replace(speech, "  ", " ")) %>%
  select(speech_id,year,speech) %>%
  na.omit() %>%
  ft_tokenizer(input_col="speech",
               output_col="tokensList") %>%
  ft_stop_words_remover(input_col = "tokensList",
                        output_col = "tokensList_no_stop_words",
                        stop_words=stop) %>%
  #filter(!is.na(tokensList_no_stop_words)) %>%
  # ft_ngram(input_col="tokensList_no_stop_words",
  #          output_col="bigramsList",
  #          n=2) %>%
  #filter(!is.na(bigramsList))%>%
  sdf_register("full_tokens_tbl")

# FILTER -------------------------------------------------
full_tokens_enviro <- full_tokens %>%
  mutate(token = explode(tokensList_no_stop_words)) %>%
  select(speech_id,
         year,
         token) %>%
  filter(nchar(token) > 2) %>%
  filter(token %IN% enviroVocab) %>%
  sdf_register("full_tokens_enviro_tbl")

full_bigrams_enviro <- full_tokens %>%
  select(speech_id,
         year,
         bigramsList) %>%
  # na.omit() %>%
  mutate(token = explode(bigramsList)) %>%
  select(speech_id,
         year,
         token) %>%
  filter(token %IN% enviroVocab) %>%
  sdf_register("full_bigrams_enviro_tbl") 

#combine token match rows with bigram match rows and find unique speech_id's  
full_enviro <- full_tokens_enviro
 # sdf_bind_rows(full_bigrams_enviro) %>%
  # na.omit() %>%
  # select(speech_id)%>%
  # distinct()%>%
  # sdf_register("full_enviro_tbl") %>%
  # compute("full_enviro_tbl")
#  compute("enviro_speeches_ids")

# 

# #find speech id's with either token or bigram match
# enviro_speeches_ids <- full_enviro %>%
#   select(speech_id) %>%
#   distinct() %>%
#   sdf_register("enviro_speeches_ids_tbl")

#filter full speech datasat to just enviro speeches
enviro_speeches <- full_tokens %>%
  semi_join(full_enviro, na_matches="never") %>%
  select(-speech) %>%
  sdf_register("enviro_speeches_tbl") #%>%
# collect()
#write_csv(enviro_speeches,"enviro_speeches.csv")

# enviro_speeches %>%
#   summarize(year,max)

enviro_tokens <- enviro_speeches %>%
  mutate(token = explode(tokensList_no_stop_words)) %>%
  select(speech_id,
         year,
         token) %>%
  filter(nchar(token) > 2) %>%
  sdf_register("enviro_tokens_tbl")

enviro_bigrams <- enviro_speeches %>%
  select(speech_id,
         year,
         bigramsList) %>%
  # na.omit() %>%
  mutate(token = explode(bigramsList)) %>%
  select(speech_id,
         year,
         token) %>%
  sdf_register("enviro_bigrams_tbl") 

#enviros list word count by year
enviro_count <- enviro_tokens %>%
  group_by(year) %>%
  count(token) %>% 
  slice_max(order_by=n,n=15) %>%
  arrange(year) %>%
  sdf_register("token_count_tbl") 
  #collect()
write_csv(enviro_count,"counts.csv")

# sizeSpeeches <- enviro_speeches %>% 
#   select(speech_id) %>%
#   count() %>%
#   sdf_register("sizeSpeeches_tbl")

sizeYear <- enviro_speeches %>% 
  select(year) %>%
  count(year) %>%
  collect()
write_csv(sizeYear,"sizeYear.csv")
#sdf_register("sizeYear_tbl")

# 
# 
# 
# 
# # TESTING -------------------------------------------------
# # tic()
# # saveRDS(collect(daily_descr), file="data/daily_descr.RDS", compress=TRUE)
# # toc()
# # tic()
# # saveRDS(collect(daily_speeches), file="data/daily_speeches.RDS", compress=TRUE)
# # toc()
# # tic()
# # saveRDS(collect(bound_descr), file="data/bound_descr.RDS", compress=TRUE)
# # toc()
# # tic()
# # saveRDS(collect(bound_speeches), file="data/bound_speeches.RDS", compress=TRUE)
# # toc()
# 
# # daily_full <- load("data/daily_full.RDS")
# # bound_full <- load("data/bound_full.RDS")
# 
# # full %>% 
# #   invoke("write") %>% 
# #   invoke("saveAsTable", as.character("full_tbl"))
# # #full <- tbl("full_tbl")
# 
# daily_descr <- spark_read_parquet(sc, 
#                                   path="data/hein-daily/descr/",
#                                   name="daily_full")
# 
# spark_read_parquet(sc, "a_dataset", "path/to/parquet/dir")
# 
# spark_write_parquet(sc, 
#                     "a_dataset", 
#                     mode="overwrite")
# 
# daily_descr <- spark_read_csv(
#   path = "data/hein-daily/descr/",
#   sc = sc, 
#   name = "daily_descr_tbl", 
#   delimiter = "|", 
#   overwrite=TRUE,
#   header = TRUE,
#   memory = FALSE)

enviro_tokens <- enviro_speeches %>%
  mutate(token = explode(tokensList_no_stop_words)) %>%
  select(speech_id,
         year,
         token) %>%
  filter(nchar(token) > 2) %>%
  filter(token %IN% enviroVocab) %>%
  sdf_register("enviro_tokens_tbl")

enviro_bigrams <- enviro_speeches %>%
  select(speech_id,
         year,
         bigramsList) %>%
  # na.omit() %>%
  mutate(token = explode(bigramsList)) %>%
  select(speech_id,
         year,
         token) %>%
  filter(token %IN% enviroVocab) %>%
  sdf_register("enviro_bigrams_tbl") 


spark_disconnect(sc)
