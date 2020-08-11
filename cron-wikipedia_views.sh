#!/bin/bash

WORKING_DIR="/home/SOC.NORTHWESTERN.EDU/bmh1867/covid19"
cd $WORKING_DIR

TZ="UTC"
date_string=${OVERRIDE_DATE_STRING:-$(date +%Y%m%d)}

view_log="enwp-daily_views-${date_string}.log"
./wikipedia/scripts/wikiproject_scraper.py 2> >(tee wikipedia/logs/${view_log})

# get the list of files
./wikipedia/scripts/fetch_enwiki_daily_views.py -d "${date_string}" 2> >(tee -a wikipedia/logs/${view_log})
mv wikipedia/logs/${view_log} /var/www/covid19/wikipedia/logs/${view_log}
mv wikipedia/data/digobs_covid19-wikipedia-enwiki_dailyviews-${date_string}.tsv /var/www/covid19/wikipedia/

# xz wikipedia/data/digobs_covid19-wikipedia-enwiki_dailyviews-${date_string}.json
mv wikipedia/data/digobs_covid19-wikipedia-enwiki_dailyviews-${date_string}.json /var/www/covid19/wikipedia/

