### COVID-19 Digital Observatory
### 
### Minimal example analysis file using revisions data.
###
### Note: The revisions data files can get quite big and it may not make
### sense to try to load them into memory in R. This example file
### loads the first 1,000 rows of a sample file.

library(tidyverse)
library(lubridate)
library(scales)

### Import and cleanup first 1000 rows of one datafile from the observatory

DataURL <-
  url("https://covid19.communitydata.science/datasets/wikipedia/digobs_covid19-wikipedia-enwiki_revisions-20200401.tsv")

revisions <- read.table(DataURL, sep="\t", header=TRUE,
                        stringsAsFactors=FALSE,
                        nrows=1000) 

revisions$timestamp <- as_datetime(revisions$timestamp)

### Group edit by editor to see edit counts
user.edit.counts <- revisions %>%
    group_by(user, title) %>% # for the example, there's only one
                                        # article title
    summarize(appearances = n()) %>%
    arrange(-appearances)

### Export that as a little table
write.table(user.edit.counts,
            file="output/user_edit_counts-first1000.csv", sep=",",
            row.names=FALSE)

### A simple time series of edits
plot <- revisions %>%
    arrange(timestamp) %>%
    mutate(count = as.numeric(rownames(revisions))) %>%
    ggplot(aes(x=timestamp, y=count)) +
    geom_jitter(alpha=0.1) +
    xlab("Date") +
    ylab("Total edits (cumulative)") +
    ggtitle(paste("Revisions to article: ",
                  head(revisions$title,1), sep="")) +
    theme_minimal()

ggsave("output/first_1000_edit_timeseries.png", plot)



