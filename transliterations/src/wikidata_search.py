# generate a list of wikidata entities related to keywords
from os import path
from sys import stdout
from wikidata_api_calls import search_wikidata, get_wikidata_api
import csv

class Wikidata_ResultSet:
    def __init__(self):
        self.results = []

    def extend(self, term, results):
        self.results.extend([Wikidata_Result(term, result, i)
                                    for i, result in enumerate(results)])

    def to_csv(self, outfile=None):
        if outfile is None:
            of = stdout

        else:
            of = open(outfile,'w',newline='')

        writer = csv.writer(of)
        writer.writerow(Wikidata_Result.__slots__)
        writer.writerows(map(Wikidata_Result.to_list, self.results))


class Wikidata_Result:
    # store unique entities found in the search results, the position in the search result, and the date
    __slots__=['search_term','entityid','pageid','search_position','timestamp']

    def __init__(self,
                 term,
                 search_result,
                 position):

        self.search_term = term.strip()
        self.entityid = search_result['title']
        self.pageid = int(search_result['pageid'])
        self.search_position = int(position)
        self.timestamp = search_result['timestamp']

    def to_list(self):
        return [self.search_term,
                self.entityid,
                self.pageid,
                self.search_position,
                self.timestamp]
    
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
