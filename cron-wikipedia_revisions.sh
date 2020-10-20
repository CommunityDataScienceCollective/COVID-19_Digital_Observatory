#!/bin/bash 

WORKING_DIR="/home/SOC.NORTHWESTERN.EDU/bmh1867/covid19"
cd $WORKING_DIR

TZ="UTC"
date_string=$(date +%Y%m%d)

revs_log="enwp-revisions-${date_string}.log"
./wikipedia/scripts/wikiproject_scraper.py 2> >(tee wikipedia/logs/${revs_log})

./wikipedia/scripts/fetch_enwiki_revisions.py 2> >(tee -a wikipedia/logs/${revs_log})
mv wikipedia/logs/${revs_log} /var/www/covid19/wikipedia/logs/

revs_tsv="digobs_covid19-wikipedia-enwiki_revisions-${date_string}.tsv"
mv wikipedia/data/${revs_tsv} /var/www/covid19/wikipedia

revs_json="digobs_covid19-wikipedia-enwiki_revisions-${date_string}.json"
xz wikipedia/data/${revs_json}
mv wikipedia/data/${revs_json}.xz /var/www/covid19/wikipedia

