#!/bin/bash -x

TZ="UTC"
date_string=$(date +%Y%m%d)

./wikipedia/scripts/wikiproject_scraper.py 2> >(tee wikipedia/logs/enwp-wikiproject_scraper-${date_string}.log)

revs_log="enwp-revisions-${date_string}.log"
./wikipedia/scripts/fetch_enwiki_revisions.py 2> >(tee wikipedia/logs/${rev_log})
cp wikipedia/logs/${rev_log} /var/www/covid19/wikipedia/logs/

revs_tsv="digobs_covid19-wikipedia-enwiki_revisions-${date_string}.tsv"
cp wikipedia/data/${revs_tsv} /var/www/covid19/wikipedia

revs_json="digobs_covid19-wikipedia-enwiki_revisions-${date_string}.json"
xz wikipedia/data/${revs_json}
cp wikipedia/data/${revs_json}.xz /var/www/covid19/wikipedia
