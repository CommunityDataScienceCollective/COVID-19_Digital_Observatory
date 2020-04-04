#!/bin/bash -x

TZ="UTC"
date_string=$(date +%Y%m%d)

revs_log="enwp-revisions-${date_string}.log"
./wikipedia/scripts/wikiproject_scraper.py 2> >(tee wikipedia/logs/${revs_log})

wd_log="wd-page-crawler-${date_string}.log"
python3 ./real-time-wiki-covid-tracker/PageCrawler.py -a "./wikipedia/resources/enwp_wikiproject_covid19_articles.txt" 2> >(tee wikipedia/logs/${wd_log})

./wikipedia/scripts/fetch_revisions.py 2> >(tee -a wikipedia/logs/${revs_log})
mv wikipedia/logs/${revs_log} /var/www/covid19/wikipedia/logs/

python3 ./wikipedia/scripts/copy_revisions_data.py ${date_string}

cd wikipedia/data
xz */${date_string}/*revisions*.json

find */${date_string}/*revisions*.xz | while read line; do
    mkdir -p /var/www/covid9/wikipedia/$line
    mv $line /var/www/covid19/wikipedia/$line
done

find */${date_string}/*revisions*.tsv | while read line; do
    mkdir -p /var/www/covid19/wikipedia/$line
    mv $line /var/www/covid19/wikipedia/$line
done

cd ../..
