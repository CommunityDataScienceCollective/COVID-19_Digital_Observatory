from wikidata_api_calls import run_sparql_query
from itertools import chain, islice
import csv
from json import JSONDecodeError
from os import path

class LabelData:
    __slots__ = ['itemid','label','langcode','is_alt']

    def __init__(self, wd_res, is_alt):
        obj = wd_res.get('label',None)
        self.label = obj.get('value',None)
        self.langcode = obj.get('xml:lang',None)
        self.itemid = wd_res.get('item',None).get('value',None)
        self.is_alt = is_alt

    def to_list(self):
        return [self.itemid,
                self.label,
                self.langcode,
                self.is_alt]

def GetAllLabels(in_csvs, outfile, topNs):

    def load_item_ids(in_csv, topN=5):
        with open(in_csv,'r',newline='') as infile:
            reader = list(csv.DictReader(infile))
            for row in reader:
                if int(row['search_position']) < topN:
                    yield row["itemid"]

    ids = set(chain(* map(lambda in_csv, topN: load_item_ids(in_csv, topN), in_csvs, topNs)))
    ids = ids.union(open("../resources/main_items.txt"))
    
    labeldata = GetItemLabels(ids)

    with open(outfile, 'w', newline='') as of:
        writer = csv.writer(of)
        writer.writerow(LabelData.__slots__)
        writer.writerows(map(LabelData.to_list,labeldata))

    
def GetItemLabels(itemids):

    def run_query_and_parse(query, is_alt):
        results = run_sparql_query(query)
        try:
            jobj = results.json()

            res = jobj.get('results',None)
            if res is not None:
                res = res.get('bindings',None)
            if res is None:
                raise requests.APIError(f"got invalid response from wikidata for {query % itemid}")

            for info in res:
                yield LabelData(info, is_alt)

        except JSONDecodeError as e:
            print(e)
            print(query)
            
    def prep_query(query, prop, itemids):
        values = ' '.join(('wd:{0}'.format(id) for id in itemids))
        return query.format(prop, values)
    
    base_query = """
    SELECT DISTINCT ?item ?label WHERE {{
    ?item {0} ?label;
    VALUES ?item  {{ {1} }}
    }}"""

    # we can't get all the items at once. how about 100 at a time?
    chunksize = 100
    itemids = (id for id in itemids)
    chunk = list(islice(itemids, chunksize))
    calls = []
    while len(chunk) > 0:
        label_query = prep_query(base_query, "rdfs:label", chunk)
        altLabel_query = prep_query(base_query, "skos:altLabel", chunk)
        label_results = run_query_and_parse(label_query,  is_alt=False)
        altLabel_results = run_query_and_parse(altLabel_query, is_alt=True)
        calls.extend([label_results, altLabel_results])
        chunk = list(islice(itemids, chunksize))

    return chain(*calls)
        

def find_new_output_file(output, i = 1):
    if path.exists(output):
        name, ext = path.splitext(output)

        return find_new_output_file(f"{name}_{i}{ext}", i+1)
    else:
        return output

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser("Use wikidata to find translations of terms")
    parser.add_argument('inputs', type=str, nargs='+', help='one or more files to read. the inputs are generated by wikidata_search.py')
    parser.add_argument('--topN', type=int, nargs='+', help='limit number of wikidata search results to use, can pass one arg for each source.')
    parser.add_argument('--output', type=str, help='an output file. defaults to stdout',default=20)

    args = parser.parse_args()

    output = find_new_output_file(args.output)

    GetAllLabels(args.inputs, output, topNs=args.topN)
