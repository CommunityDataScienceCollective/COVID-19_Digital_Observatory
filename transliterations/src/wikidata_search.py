# generate a list of wikidata entities related to keywords
from os import path
from sys import stdout
from wikidata_api_calls import search_wikidata, get_wikidata_api
import csv
from itertools import chain

class Wikidata_ResultSet:
    def __init__(self):
        self.results = []

    def extend(self, term, results):
        self.results.append(
            (Wikidata_Result(term, result, i)
             for i, result in enumerate(results))
        )

    def to_csv(self, outfile=None):
        if outfile is None:
            of = stdout

        else:
            of = open(outfile,'w',newline='')
        writer = csv.writer(of)
        writer.writerow(Wikidata_Result.__slots__)
        writer.writerows(map(Wikidata_Result.to_list, chain(* self.results)))


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
    
def run_wikidata_searches(terms):
    api = get_wikidata_api()
    resultset = Wikidata_ResultSet()
    for term in terms:
        search_results = search_wikidata(api, term)
        resultset.extend(term, search_results)
    return resultset

def read_google_trends_files(terms_files):
    def _read_file(infile):
        return csv.DictReader(open(infile,'r',newline=''))

    for row in chain(* [_read_file(terms_file) for terms_file in terms_files]):
        yield row['query']


def trawl_google_trends(terms_files, outfile = None):
    terms = read_google_trends_files(terms_files)
    resultset = run_wikidata_searches(terms)
    resultset.to_csv(outfile)

def trawl_base_terms(infiles, outfile = None):
    terms = chain(* (open(infile,'r') for infile in infiles))
    resultset = run_wikidata_searches(terms)
    resultset.to_csv(outfile)

    ## search each of the base terms in wikidata

    # store unique entities found in the search results, the position in the search result, and the date

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser("Search wikidata for entities related to a set of terms.")
    parser.add_argument('inputs', type=str, nargs='+', help='one or more files to read')
    parser.add_argument('--use-gtrends', action='store_true', help = 'toggle whether the input is the output from google trends')
    parser.add_argument('--output', type=str, help='an output file. defaults to stdout')
    args = parser.parse_args()
    if args.use_gtrends:
        trawl_google_trends(args.inputs, args.output)
    else:
        trawl_base_terms(args.inputs, args.output)
