#####cran: https://cran.r-project.org/web/packages/congress/congress.pdf
#####NOTE: 
options(repos = list(CRAN="http://cran.rstudio.com/"))
rm(list=ls())
#####INSTALL PACKAGES

# install.packages("Rcpp", dependencies=TRUE)
# library(Rcpp)
# install.packages("fs",dependencies=TRUE)
# library(fs)
# p_load(libgit2)
# p_del(processx)
# # Installing previous verison 3.5.1
# install.packages("https://cran.r-project.org/src/contrib/Archive/processx/processx_3.6.1.tar.gz", repos=NULL, type="source")
# library(processx)

install.packages("pacman")
library(pacman)
p_load(devtools)
p_load(tidyverse, tidytext, congress, httr, federalregister, readxl, xml2, sparklyr, rvest, purrr, here)
devtools::install_github("judgelord/legislators")
library(legislators)
devtools::install_github("judgelord/congressionalrecord")
library(congressionalrecord)

#ARCHIVE --------------------------------------------------
# urlBase <- "https://www.congress.gov/congressional-record/"
# x <- GET(urlBase, add_headers('user-agent' = 'Congress data scraper'))

# spark_install(version = "3.4")
# config <- spark_config()
# config["sparklyr.shell.driver-memory"] <- "16G"
# config["sparklyr.shell.executor-memory"] <- "16G"
# config$spark.yarn.executor.memoryOverhead <- "10g"
# sc <- spark_connect(master = "local", config = config)
# 

# # a function to make a data frame of of all cr text urls for a date
# get_cr_df2 <- function(date, section){
# 
#   url <- str_c("https://www.congress.gov/congressional-record",
#                date %>% str_replace_all("-", "/"),
#                section, sep = "/") %>%
#     GET(.,user_agent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"),
#         add_headers("token-auth" = "n2xYrGZYXIhatI4gCQkIs4cwbRMckUwvpqM0brkY"))
# 
#   pages <- read_html(url) %>%
#     html_nodes("a")# "a" nodes are linked text
# 
#   d <- tibble(header = html_text(pages), # the text of the linked text
#               date = date,
#               section = section,
#               url = str_c("https://www.congress.gov",
#                           html_attr(pages, "href") # urls are "href" attributes of linked text
#               )
#   ) %>%
#     # trim down to html txt pages
#     filter(url %>% str_detect("article"))
# 
#   return(d)
#   Sys.sleep(3)
# }

# get_cr_df2 <- function(date,section) {
#   get_cr_df(date,section)
#   Sys.sleep(3)
# }

#SET-UP --------------------------------------------------
"api_key" = "n2xYrGZYXIhatI4gCQkIs4cwbRMckUwvpqM0brkY"
 urlBase <- "https://www.congress.gov/congressional-record/"
 x <- GET(urlBase, user_agent('Congress data scraper'))

# # an empty dataframe for failed calls
d_init <-  tibble(header = "",
                  date = as.Date(NA),
                  section = "",
                  url = "")

# For testing
# section <- "senate-section"
# date <- "2020-09-15"
# get_cr_df(date, section)



#2024 --------------------------------------------------
# a date range to scrape
dates <- seq(as.Date("2024/01/01"), 
                 Sys.Date(), # today
             by = "day") 

# a dataframe of headers, dates, and url paths
senate_2024 <- map_dfr(dates, .f= {
  Sys.sleep(3)
  possibly(get_cr_df,
           otherwise=d_init,
           quiet=FALSE)},
  section="senate-section")

house_2024 <- map_dfr(dates, .f= {
  Sys.sleep(3)
  possibly(get_cr_df,
           otherwise=d_init,
           quiet=FALSE)},
  section="house-section")

ext_2024 <- map_dfr(dates, .f= {
  Sys.sleep(3)
  possibly(get_cr_df,
           otherwise=d_init,
           quiet=FALSE)},
  section="extensions-of-remarks-section") 

cr_metadata_2024 <- bind_rows(senate_2024, house_2024) %>%
  bind_rows(ext_2024)%>%
  na.omit() 

save(cr_metadata_2024,file="data/cr_metadata_2024.Rdata")

#2023 --------------------------------------------------
# a date range to scrape
dates <- seq(as.Date("2023/01/01"), 
             as.Date("2023/12/31"),
             by = "day") 

# a dataframe of headers, dates, and url paths
senate_2023 <- map_dfr(dates, .f= {
  Sys.sleep(8)
  possibly(get_cr_df,
           otherwise=d_init,
           quiet=FALSE)},
  section="senate-section")

house_2023 <- map_dfr(dates, .f= {
  Sys.sleep(3)
  possibly(get_cr_df,
           otherwise=d_init,
           quiet=FALSE)},
  section="house-section")

ext_2023 <- map_dfr(dates, .f= {
  Sys.sleep(3)
  possibly(get_cr_df,
           otherwise=d_init,
           quiet=FALSE)},
  section="extensions-of-remarks-section") 

cr_metadata_2023 <- bind_rows(senate_2023, house_2023) %>%
  bind_rows(ext_2023)%>%
  na.omit() 

write_csv(cr_metadata_2023,"cr_metadata_2023.csv")
save(cr_metadata_2023,file="data/cr_metadata_2023.Rdata")
#2022 --------------------------------------------------
# a date range to scrape
dates <- seq(as.Date("2022/01/01"), 
             as.Date("2022/12/31"),
             by = "day") 

# a dataframe of headers, dates, and url paths
senate_2022 <- map_dfr(dates, .f= {
  Sys.sleep(8)
  possibly(get_cr_df,
           otherwise=d_init,
           quiet=FALSE)},
  section="senate-section")

house_2022 <- map_dfr(dates, .f= {
  Sys.sleep(3)
  possibly(get_cr_df,
           otherwise=d_init,
           quiet=FALSE)},
  section="house-section")

ext_2022 <- map_dfr(dates, .f= {
  Sys.sleep(3)
  possibly(get_cr_df,
           otherwise=d_init,
           quiet=FALSE)},
  section="extensions-of-remarks-section") 

cr_metadata_2022 <- bind_rows(senate_2022, house_2022) %>%
  bind_rows(ext_2022)%>%
  na.omit() 

write_csv(cr_metadata_2022,"cr_metadata_2022.csv")
save(cr_metadata_2022,file="data/cr_metadata_2022.Rdata")
#2021 --------------------------------------------------
# a date range to scrape
dates <- seq(as.Date("2021/01/01"), 
             as.Date("2021/12/31"),
             #    Sys.Date(), # today
             by = "day") 

# a dataframe of headers, dates, and url paths
senate_2021 <- map_dfr(dates, .f= {
  Sys.sleep(3)
  possibly(get_cr_df,
             otherwise=d_init,
             quiet=FALSE)},
    section="senate-section")

house_2021 <- map_dfr(dates, .f= {
  Sys.sleep(3)
  possibly(get_cr_df,
           otherwise=d_init,
           quiet=FALSE)},
  section="house-section")

ext_2021 <- map_dfr(dates, .f= {
  Sys.sleep(3)
  possibly(get_cr_df,
           otherwise=d_init,
           quiet=FALSE)},
  section="extensions-of-remarks-section") 

cr_metadata_2021 <- bind_rows(senate_2021, house_2021) %>%
  bind_rows(ext_2021)%>%
  na.omit() 
write_csv(cr_metadata_2021,"cr_metadata_2021.csv")

#2020 --------------------------------------------------
# a date range to scrape
dates <- seq(as.Date("2020/01/01"), 
             as.Date("2020/12/31"),
             by = "day") 

# a dataframe of headers, dates, and url paths
senate_2020 <- map_dfr(dates, .f= {
  Sys.sleep(8)
  possibly(get_cr_df,
           otherwise=d_init,
           quiet=FALSE)},
  section="senate-section")

house_2020 <- map_dfr(dates, .f= {
  Sys.sleep(3)
  possibly(get_cr_df,
           otherwise=d_init,
           quiet=FALSE)},
  section="house-section")

ext_2020 <- map_dfr(dates, .f= {
  Sys.sleep(3)
  possibly(get_cr_df,
           otherwise=d_init,
           quiet=FALSE)},
  section="extensions-of-remarks-section") 

cr_metadata_2020 <- bind_rows(senate_2020, house_2020) %>%
  bind_rows(ext_2020)%>%
  na.omit() 

write_csv(cr_metadata_2020,"cr_metadata_2020.csv")
save(cr_metadata_2020,file="data/cr_metadata_2020.Rdata")
#2019 --------------------------------------------------
# a date range to scrape
dates <- seq(as.Date("2019/01/01"), 
             as.Date("2019/12/31"),
             by = "day") 

# a dataframe of headers, dates, and url paths
senate_2019 <- map_dfr(dates, .f= {
  Sys.sleep(8)
  possibly(get_cr_df,
           otherwise=d_init,
           quiet=FALSE)},
  section="senate-section")

house_2019 <- map_dfr(dates, .f= {
  Sys.sleep(3)
  possibly(get_cr_df,
           otherwise=d_init,
           quiet=FALSE)},
  section="house-section")

ext_2019 <- map_dfr(dates, .f= {
  Sys.sleep(3)
  possibly(get_cr_df,
           otherwise=d_init,
           quiet=FALSE)},
  section="extensions-of-remarks-section") 

cr_metadata_2019 <- bind_rows(senate_2019, house_2019) %>%
  bind_rows(ext_2019)%>%
  na.omit() 

write_csv(cr_metadata_2019,"cr_metadata_2019.csv")
save(cr_metadata_2019,file="data/cr_metadata_2019.Rdata")
#2018 --------------------------------------------------
# a date range to scrape
dates <- seq(as.Date("2018/01/01"), 
             as.Date("2018/12/31"),
             by = "day") 

# a dataframe of headers, dates, and url paths
senate_2018 <- map_dfr(dates, .f= {
  Sys.sleep(3)
  possibly(get_cr_df,
           otherwise=d_init,
           quiet=FALSE)},
  section="senate-section")

house_2018 <- map_dfr(dates, .f= {
  Sys.sleep(3)
  possibly(get_cr_df,
           otherwise=d_init,
           quiet=FALSE)},
  section="house-section")

ext_2018 <- map_dfr(dates, .f= {
  Sys.sleep(3)
  possibly(get_cr_df,
           otherwise=d_init,
           quiet=FALSE)},
  section="extensions-of-remarks-section") 

cr_metadata_2018 <- bind_rows(senate_2018, house_2018) %>%
  bind_rows(ext_2018)%>%
  na.omit() 

write_csv(cr_metadata_2018,"cr_metadata_2018.csv")
save(cr_metadata_2018,file="data/cr_metadata_2018.Rdata")
#2017 --------------------------------------------------
# a date range to scrape
dates <- seq(as.Date("2017/01/01"), 
             as.Date("2017/12/31"),
             by = "day") 

# a dataframe of headers, dates, and url paths
senate_2017 <- map_dfr(dates, .f= {
  Sys.sleep(3)
  possibly(get_cr_df,
           otherwise=d_init,
           quiet=FALSE)},
  section="senate-section")

house_2017 <- map_dfr(dates, .f= {
  Sys.sleep(3)
  possibly(get_cr_df,
           otherwise=d_init,
           quiet=FALSE)},
  section="house-section")

ext_2017 <- map_dfr(dates, .f= {
  Sys.sleep(3)
  possibly(get_cr_df,
           otherwise=d_init,
           quiet=FALSE)},
  section="extensions-of-remarks-section") 

cr_metadata_2017 <- bind_rows(senate_2017, house_2017) %>%
  bind_rows(ext_2017)%>%
  na.omit() 

write_csv(cr_metadata_2017,"cr_metadata_2017.csv")
save(cr_metadata_2017,file="data/cr_metadata_2017.Rdata")
#2016 --------------------------------------------------


#SCRAPING TEXT --------------------------------------------------

# d <- cr_metadata
load(here::here("data", "cr_metadata_2019.Rdata"))
# cr_metadata_2024 %<>% full_join(d)
# save(cr_metadata, file = here::here("data", "cr_metadata.Rdata"))

# already downloaded 
#downloaded <- list.files(here::here("data", "htm"))

# a function to download htm
get_cr_htm2 <- function(url){
  ## test
  # url <- "https://www.congress.gov/congressional-record/2020/03/02/senate-section/article/S1255-1"
  
  # follow the link to the txt htm 
  url %<>% 
    session() %>%
    session_follow_link("View TXT in new window")
  
  # name files the end of the url 
  file <- str_remove(url$url, ".*modified/")
  
  # if the file has not already been downloaded
  if(!file %in% downloaded){
    read_html(url) %>% 
      write_html(file = here::here("data","htm", file))
  }
}

## test 
# get_cr_htm(cr_metadata$url[1])

# download file for each url 
cr_metadata <- cr_metadata_2019%>%
  filter(date=="2019-01-08")
walk(cr_metadata$url, get_cr_htm2)
