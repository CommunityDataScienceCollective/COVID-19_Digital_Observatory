### COVID-19 Digital Observatory
### 2020-03-28
### 
### Minimal example analysis file using pageview data

library(tidyverse)
library(ggplot2)
library(scales)

### Import and cleanup data

DataURL <-
    url("https://github.com/CommunityDataScienceCollective/COVID-19_Digital_Observatory/raw/master/wikipedia_views/data/dailyviews2020032600.tsv")

views <-
    read.table(DataURL, sep="\t", header=TRUE, stringsAsFactors=FALSE) 

### Alternatively, uncomment and run if working locally with full git
### tree
###
### Identify data source directory and file
## DataDir <- ("../data/")
## DataFile <- ("dailyviews2020032600.tsv")

## related.searches.top <- read.table(paste(DataDir,DataFile, sep=""),
##                                   sep="\t", header=TRUE,
##                                   stringsAsFactors=FALSE)

### Cleanup and do the grouping with functions from the Tidyverse
### (see https://www.tidyverse.org for more info)

views <- views[,c("article", "project", "timestamp", "views")]
views$timestamp <- factor(views$timestamp)

### Sorts and groups at the same time
views.by.proj.date <- arrange(group_by(views, project, timestamp),
                        desc(views))

### Export just the top 10 by pageviews
write.table(head(views.by.proj.date, 10),
            file="output/top10_views_by_project_date.csv", sep=",",
            row.names=FALSE)

### A simple visualization
p <- ggplot(data=views.by.proj.date, aes(views))

## Density plot with log-transformed axis
p + geom_density() + scale_x_log10(labels=comma)



