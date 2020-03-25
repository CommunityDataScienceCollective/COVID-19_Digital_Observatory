# File defines functions for making api calls to find translations and transliterations for key terms.

import mwapi
import requests
import sys
from defaults import user_agent

def get_wikidata_api():
    session = mwapi.Session(host="https://wikidata.org/w/api.php", user_agent=user_agent)
    return session

def search_wikidata(session, term, *args, **kwargs):
    search_results = session.get(action='query',
                                 list='search',
                                 srsearch=term,
#                                 srqiprofile='popular_inclinks_pv',
                                 srlimit='max',
                                 srnamespace=0,
                                 *args,
                                 **kwargs)


    query = search_results.get('query', None)
    results = query.get('search', None)

    if results is None:
        raise mwapi.session.APIError(f"No results for query: {term}")

    return results

def run_sparql_query(q):
    results = requests.get("https://query.wikidata.org/bigdata/namespace/wdq/sparql?query={q}&format=json")
    
