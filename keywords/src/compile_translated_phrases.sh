#!/bin/bash

# For now these scripts don't accept command line arguments. It's an MVP

echo "Reading Google trends" 
python3 collect_trends.py

echo "Searching for Wikidata items using base_terms.txt"
python3 wikidata_search.py ../resources/base_terms.txt --output ../output/intermediate/wikidata_search_results.csv

echo "Searching for Wikidata items using Google trends"
python3 wikidata_search.py ../output/intermediate/related_searches_rising.csv ../output/intermediate/related_searches_top.csv --use-gtrends --output ../output/intermediate/wikidata_search_results_from_gtrends.csv

echo "Finding translations from Wikidata using sparql"
python3 wikidata_translations.py  ../output/intermediate/wikidata_search_results_from_gtrends.csv  ../output/intermediate/wikidata_search_results.csv --topN 10 20 --output ../output/csv/$(date '+%Y-%m-%d')_wikidata_item_labels.csv

rm latest.csv
ln -s $(ls -tr | tail -n 1) latest.csv
