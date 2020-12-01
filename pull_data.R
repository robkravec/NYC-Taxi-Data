## This script file downloads yellow and green taxi data from 
## https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page for Feb 2015
## and saves the files locally for us to process using Spark

# Load libraries
library(tidyverse)

# Specify paths for aws-stored files
yellow_path <- str_c("https://s3.amazonaws.com/nyc-tlc/trip+data/", 
                     "yellow_tripdata_2015-02.csv")
green_path <- str_c("https://s3.amazonaws.com/nyc-tlc/trip+data/",
                    "green_tripdata_2015-02.csv")

# Download files, and store in data folder
download.file(url = yellow_path, destfile = "data/yellow.csv")
download.file(url = green_path, destfile = "data/green.csv")
