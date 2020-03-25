from wikidata_api_calls import run_sparql_query
from itertools import chain, islice
import csv
from json import JSONDecodeError

class LabelData:
    __slots__ = ['entityid','label','langcode','is_alt']

    def __init__(self, wd_res, entityid, is_alt):
        obj = wd_res.get('label',None)
        self.label = obj.get('value',None)
        self.langcode = obj.get('xml:lang',None)
        self.entityid = entityid
        self.is_alt = is_alt

    def to_list(self):
        return [self.entityid,
                self.label,
                self.langcode,
                self.is_alt]


def GetAllLabels(in_csv, outfile, topN):

    def load_entity_ids(in_csv, topN=5):
        with open(in_csv,'r',newline='') as infile:
            reader = csv.DictReader(infile)
            for row in reader:
                if int(row['search_position']) < topN:
                    yield row["entityid"]

    ids = set(load_entity_ids(in_csv, topN))

    labeldata = chain(* map(GetEntityLabels, ids))

    with open(outfile, 'w', newline='') as of:
        writer = csv.writer(of)
        writer.writerow(LabelData.__slots__)
        writer.writerows(map(LabelData.to_list,labeldata))

    
def GetEntityLabels(entityid):

    def run_query_and_parse(query, entityid, is_alt):
        results = run_sparql_query(query % entityid)
        try:
            jobj = results.json()
            res = jobj.get('results',None)
            if res is not None:
                res = res.get('bindings',None)
            if res is None:
                raise requests.APIError(f"got invalid response from wikidata for {query % entityid}")
            for info in res:
                yield LabelData(info, entityid, is_alt)

        except JSONDecodeError as e:
            print(e)
            print(query % entityid)
            

    label_base_query = """
    SELECT DISTINCT ?label WHERE {
    wd:%s rdfs:label ?label;
    }"""

    altLabel_base_query = """
    SELECT DISTINCT ?label WHERE {
    wd:%s skos:altLabel ?label;
    }"""

    label_results = run_query_and_parse(label_base_query, entityid, is_alt=False)

    altLabel_results = run_query_and_parse(altLabel_base_query, entityid, is_alt=True)

    return chain(label_results, altLabel_results)
        

if __name__ == "__main__":
    GetAllLabels("../data/output/wikidata_search_results.csv","../data/output/wikidata_entity_labels.csv", topN=20)
