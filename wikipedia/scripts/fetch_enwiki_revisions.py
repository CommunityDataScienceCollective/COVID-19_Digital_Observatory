#!/usr/bin/env python3

###############################################################################
#
# This script assumes the presence of the COVID-19 repo.
# 
# It (1) reads in the article list and then (2) calls the Wikimedia API to 
# fetch view information for each article. Output is to (3) JSON and TSV.
#
###############################################################################

import argparse
import logging
import os.path
import json
import datetime

from requests import Request
from csv import DictWriter
from mw import api
import digobs


def parse_args():

    parser = argparse.ArgumentParser(description='Call the views API to collect Wikipedia revision data.')
    parser.add_argument('-o', '--output_folder', help='Where to save output', default="wikipedia/data", type=str)
    parser.add_argument('-i', '--article_file', help='File listing article names', default="wikipedia/resources/enwp_wikiproject_covid19_articles.txt", type=str)
    parser.add_argument('-L', '--logging_level', help='Logging level. Options are debug, info, warning, error, critical. Default: info.', default='info', type=digobs.get_loglevel), 
    parser.add_argument('-W', '--logging_destination', help='Logging destination file. (default: standard error)', type=str), 
    args = parser.parse_args()
    return(args)

def main():
    args = parse_args()

    output_path = args.output_folder
    article_filename = args.article_file

    #handle -W
    if args.logging_destination:
        logging.basicConfig(filename=args.logging_destination, filemode='a', level=args.logging_level)
    else:
        logging.basicConfig(level=args.logging_level)

    export_time = str(datetime.datetime.now())
    export_date = datetime.datetime.today().strftime("%Y%m%d")

    logging.info(f"Starting run at {export_time}")
    logging.info(f"Last commit: {digobs.git_hash()}")

    json_output_filename = os.path.join(output_path, f"digobs_covid19-wikipedia-enwiki_revisions-{export_date}.json")
    tsv_output_filename =  os.path.join(output_path, f"digobs_covid19-wikipedia-enwiki_revisions-{export_date}.tsv")
    
    api_session = api.Session("https://en.wikipedia.org/w/api.php")

    # list of properties from the API we want to gather (basically all of
    # them supported by mediawik-utilities)

    rv_props =  {'revid' : 'ids',
                 'timestamp' : 'timestamp',
                 'user' : 'user',
                 'userid' : 'userid',
                 'size' : 'size',
                 'sha1' : 'sha1',
                 'contentmodel' : 'contentmodel',
                 'tags' : 'tags',
                 'flags' : 'flags',
                 'comment' : 'comment',
                 'content' : 'content' }

    exclude_from_tsv = ['tags', 'comment', 'content', 'flags']

    # load the list of articles
    with open(article_filename, 'r') as infile:
        article_list= list(map(str.strip, infile))

    def get_revisions_for_page(title):
        return api_session.revisions.query(properties=rv_props.values(),
                                           titles={title},
                                           direction="newer")

    tsv_fields = ['title', 'pageid', 'namespace']
    tsv_fields = tsv_fields + list(rv_props.keys())

    # drop fields that we identified for exclusion
    tsv_fields = [e for e in tsv_fields if e not in exclude_from_tsv]

    # add special export fields
    tsv_fields = tsv_fields + ['anon', 'minor', 'url', 'export_timestamp', 'export_commit']

    export_info = { 'git_commit' : digobs.git_hash(),
                    'timestamp' : export_time }

    with open(json_output_filename, 'w') as json_output, \
         open(tsv_output_filename, 'w') as tsv_output:

        tsv_writer = DictWriter(tsv_output, fieldnames=tsv_fields, delimiter="\t")
        tsv_writer.writeheader()

        for article in article_list:
            logging.info(f"pulling revisions for: {article}")
            
            # try to grab the code 10 times, sleeping for one minute each time
            tries = 0
            while True:
                try:
                    revisions = get_revisions_for_page(article)
                    logging.debug(f"successfully received revisions for: {article}")
                    break
                except:
                    if tries > 10:
                        logging.critical(f"giving up after 10 tries to get {article}")
                        raise
                    else:
                        logging.warning(f"socket.timeout from {article}")
                        logging.warning(f"sleeping 60 seconds before retrying")
                        tries = tries + 1
                        time.sleep(60)
                        continue
            
            for rev in revisions:
                logging.debug(f"processing raw revision: {rev}")

                # add export metadata
                rev['exported'] = export_info

                # save the json version of the code
                print(json.dumps(rev), file=json_output)

                # handle missing data
                if "sha1" not in rev:
                    rev["sha1"] = ""

                if "userhidden" in rev:
                    rev["user"] = ""
                    rev["userid"] = ""

                # recode anon so it's true or false instead of present/missing
                if "anon" in rev:
                    rev["anon"] = True
                else:
                    rev["anon"] = False
                    
                # let's recode "minor" in the same way
                if "minor" in rev:
                    rev["minor"] = True
                else:
                    rev["minor"] = False

                # add page title information
                rev['title'] = rev['page']['title']
                rev['pageid'] = rev['page']['pageid']
                rev['namespace'] = rev['page']['ns']

                # construct a URL
                rev['url'] = Request('GET', 'https://en.wikipedia.org/w/index.php',
                                     params={'title' : rev['title'].replace(" ", "_"),
                                            'oldid' : rev['revid']}).prepare().url

                rev['export_timestamp'] = export_time
                rev['export_commit'] = digobs.git_hash(short=True)

                tsv_writer.writerow({k: rev[k] for k in tsv_fields})

if __name__ == "__main__":
    main()
