# Keywords

This code finds trending web searches related to the COVID-19 pandemic using Google trends (`collect_trends.py`). It then searches for relevant keywords on Wikidata (`wikidata_search`) in order to find high-quality translations of important words and phrases (`wikidata_translations.py`). The goal is to support efforts expanding the Observatory to information in many languages beyond English.  

We search the Wikidata API for entities in `src/wikidata_search.py` and then we make simple SPARQL queries in `src/wikidata_translations.py` to collect labels and aliases the entities.  The labels come with language metadata.  This seems to provide a decent initial list of relevant terms across multiple languages. 

The output data lives at [covid19.communitydata.science](https://covid19.communitydata.science/datasets/keywords).

The output files have 4 colums: 

- `itemid` links to the wikidata entity
- `label` is the translation of the relevant keyword
- `langcode` is the [iso 639-1](https://en.wikipedia.org/wiki/List_of_ISO_639-1_codes) code corresponding the language of the label. 
- `is_alt` indicates whether the label is an [alias](https://www.wikidata.org/wiki/Help:Aliases).
