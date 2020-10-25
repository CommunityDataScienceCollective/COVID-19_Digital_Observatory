import mwapi
from urllib.parse import urlparse
import argparse
from defaults import user_agent

parser = argparse.ArgumentParser()
parser.add_argument("--url", help='url to mediawiki page with list of entities to extract',default='https://meta.wikimedia.org/wiki/User:MGerlach_(WMF)/covid_related_pages_reading_sessions')
parser.add_argument("--output", help='path_to_output file', type=str, default='../output/intermediate/Mgerlach_wikidata_ids.txt')

args = parser.parse_args()

url = args.url
urlparsed = urlparse(url)
hostname = urlparsed.netloc

api = mwapi.Session("https://" + hostname,user_agent=user_agent)
page = urlparsed.path.replace("/wiki/","")

res = api.request(method='get',params={'action':'parse','page':page,'format':'json'})

res.keys()
res['parse'].keys()
links = res['parse']['iwlinks']

wikidata_links = filter(lambda l: l['prefix'] == "wikidata",links)
item_ids = map(lambda l: l['*'].split('/')[-1].split(':')[-1], wikidata_links)
item_ids = set(item_ids)
with open(args.output,'w') as of:
    of.write("itemid\n")
    of.writelines(map(lambda l: l + "\n", item_ids))
