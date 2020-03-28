#!/bin/bash

# For now these scripts don't accept command line arguments. It's an MVP

echo "Reading Google trends"
python3 collect_trends.py

echo "Searching for Wikidata entities using base_terms.txt"
python3 wikidata_search.py ../data/input/base_terms.txt --output ../data/output/wikidata_search_results.csv

echo "Searching for Wikidata entities using Google trends"
python3 wikidata_search.py ../data/output/related_searches_rising.csv ../data/output/related_searches_top.csv --use-gtrends --output ../data/output/wikidata_search_results_from_gtrends.csv

echo "Finding transliterations from Wikidata using sparql"
python3 wikidata_transliterations.py  ../data/output/wikidata_search_results_from_gtrends.csv  ../data/output/wikidata_search_results.csv --topN 10 20 --output ../data/output/$(date '+%Y-%m-%d')_wikidata_entity_labels.csv

