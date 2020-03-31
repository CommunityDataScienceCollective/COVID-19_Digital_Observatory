# Transliterations

This part of the project collects tranliterations of key phrases related to COVID-19 using Wikidata.  We search the Wikidata API for entities in `src/wikidata_search.py` and then we make simple SPARQL queries in `src/wikidata_transliterations.py` to collect labels and aliases the entities.  The labels come with language metadata.  This seems to provide a decent initial list of relevant terms across multiple languages. 
