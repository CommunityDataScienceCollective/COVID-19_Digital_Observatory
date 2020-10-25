#!/bin/bash

WORKING_DIR="/data/users/bmh1867/covid19"
cd $WORKING_DIR

TZ="UTC"
date_string=${OVERRIDE_DATE_STRING:-$(date +%Y%m%d -d "yesterday")}

view_log="daily_views-${date_string}.log"
./wikipedia/scripts/wikiproject_scraper.py 2> >(tee wikipedia/logs/${view_log})

wd_log="wd-page-crawler-${date_string}.log"
python3 ./real-time-wiki-covid-tracker/PageCrawler.py -a "./wikipedia/resources/enwp_wikiproject_covid19_articles.txt" 2> >(tee wikipedia/logs/${wd_log})

# get the list of files
./wikipedia/scripts/fetch_daily_views.py -d "${date_string}" 2> >(tee -a wikipedia/logs/${view_log})
mv wikipedia/logs/${view_log} /var/www/covid19/wikipedia/logs/${view_log}

cd wikipedia/data
find */${date_string}/*dailyviews*.tsv | while read line; do
    mkdir -p /var/www/covid19/wikipedia/$line
    mv $line /var/www/covid19/wikipedia/$line
done

find */${date_string}/*dailyviews*.json | while read line; do
    mkdir -p /var/www/covid19/wikipedia/$line
    mv $line /var/www/covid19/wikipedia/$line
done

cd ../..
