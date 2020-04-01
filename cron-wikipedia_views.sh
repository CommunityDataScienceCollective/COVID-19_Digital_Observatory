#!/bin/bash -x

TZ="UTC"
date_string=$(date +%Y%m%d)

./wikipedia/scripts/wikiproject_scraper.py 2> >(tee wikipedia/logs/enwp-wikiproject_scraper-${date_string}.log)

# get the list of files
view_log="enwp-daily_views-${date_string}.log"
./wikipedia/scripts/fetch_enwiki_daily_views.py 2> >(tee wikipedia/logs/${view_log})
cp wikipedia/logs/${view_log} /var/www/covid19/wikipedia/logs/${view_log}
cp wikipedia/data/digobs_covid19-wikipedia-enwiki_dailyviews-${date_string}.tsv /var/www/covid19/wikipedia/

# xz wikipedia/data/digobs_covid19-wikipedia-enwiki_dailyviews-${date_string}.json
cp wikipedia/data/digobs_covid19-wikipedia-enwiki_dailyviews-${date_string}.json /var/www/covid19/wikipedia/

