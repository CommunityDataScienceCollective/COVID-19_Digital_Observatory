### COVID-19 Digital Observatory
### 2020-03-28
### 
### Minimal example analysis file using trending search data

### Identify data source directory and file
DataDir <- ("../data/output/")
DataFile <- ("related_searches_top.csv")

### Import and cleanup data
related.searches.top <- read.table(paste(DataDir,DataFile,
                                 sep=""),
                           sep=",", header=TRUE,
                           stringsAsFactors=FALSE) 

### Aggregate top 5 search queries by term/day
top5.per.term.date <- aggregate(query ~ term + date,
                                data=related.searches.top,
                                head, 5)

## Might cleanup a bit for further analysis or visualization...
top5.per.term.date$date <- as.Date(top5.per.term.date$date)

### Export
write.table(top5.per.term.date,
            file="output/top5_queries_per_term_per_date.csv", sep=",",
            row.names=FALSE)

