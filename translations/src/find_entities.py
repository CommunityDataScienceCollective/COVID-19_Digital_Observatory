# generate a list of wikidata entities related to keywords
from os import path
from sys import stdout
from wikidata_api_calls import search_wikidata

class Wikidata_ResultSet(object):
    def __init__(self):
        self.results = []

    def extend(self, term, results):
        self.results.extend([Wikidata_Result(term, result, i)
                                    for i, result in enumerate(results)])

    def to_csv(self, outfile=None):
        
        header = ','.join(['search_term', 'entityid', 'pageid', 'search_position','timestamp'])
        if outfile is None:
            of = stdout

        else:
            of = open(outfile,'w')

        of.write(header)
        for result in self.results:
            of.write(result.to_csv())

        of.close()


class Wikidata_Result(object):
    # store unique entities found in the search results, the position in the search result, and the date
    __slots__=['search_term','entityid','pageid','search_position','timestamp']

    def __init__(self,
                 term,
                 search_result,
                 position):

        self.search_term = term.strip()
        self.entityid = search_result['title']
        self.pageid = search_result['pageid']
        self.search_position = position
        self.timestamp = search_result['timestamp']

    def to_csv(self):
        return ','.join([self.search_term,
                         self.entityid,
                         str(self.pageid),
                         str(self.search_position),
                         str(self.timestamp)]) + '\n'
    
def run_wikidata_searches(terms_file = '../data/input/base_terms.txt', outfile="../data/output/wikidata_search_results.csv"):

    resultset = Wikidata_ResultSet()
    for term in open(terms_file,'r'):
        api = get_wikidata_api()
        search_results = search_wikidata(api, term)
        resultset.extend(term, search_results)

    resultset.to_csv(outfile)


    ## search each of the base terms in wikidata

    # store unique entities found in the search results, the position in the search result, and the date

if __name__ == "__main__":
    run_wikidata_searches()
