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
import subprocess
import datetime

from requests import Request
from csv import DictWriter
from mw import api


def parse_args():

    parser = argparse.ArgumentParser(description='Call the views API to collect Wikipedia revision data.')
    parser.add_argument('-o', '--output_folder', help='Where to save output', default="wikipedia/data", type=str)
    parser.add_argument('-i', '--article_file', help='File listing article names', default="wikipedia/resources/enwp_wikiproject_covid19_articles.txt", type=str)
    parser.add_argument('-L', '--logging_level', help='Logging level. Options are debug, info, warning, error, critical. Default: info.', default='info', type=str), 
    parser.add_argument('-W', '--logging_destination', help='Logging destination file. (default: standard error)', type=str), 
    args = parser.parse_args()
    return(args)

def main():
    args = parse_args()

    output_path = args.output_folder
    article_filename = args.article_file

    #handle -L
    loglevel_mapping = { 'debug' : logging.DEBUG,
                         'info' : logging.INFO,
                         'warning' : logging.WARNING,
                         'error' : logging.ERROR,
                         'critical' : logging.CRITICAL }

    if args.logging_level in loglevel_mapping:
        loglevel = loglevel_mapping[args.logging_level]
    else:
        print("Choose a valid log level: debug, info, warning, error, or critical") 
        exit

    #handle -W
    if args.logging_destination:
        logging.basicConfig(filename=args.logging_destination, filemode='a', level=loglevel)
    else:
        logging.basicConfig(level=loglevel)

    export_git_hash = subprocess.check_output(['git', 'rev-parse', 'HEAD']).decode().strip()
    export_git_short_hash = subprocess.check_output(['git', 'rev-parse', '--short', 'HEAD']).decode().strip()
    export_time = str(datetime.datetime.now())
    export_date = datetime.datetime.today().strftime("%Y%m%d")

    logging.info(f"Starting run at {export_time}")
    logging.info(f"Last commit: {export_git_hash}")

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
                 'comment' : 'comment',
                 'content' : 'content' }

    exclude_from_tsv = ['tags', 'comment', 'content']

    # load the list of articles
    with open(article_filename, 'r') as infile:
        article_list = [art.strip() for art in list(infile)]

    def get_revisions_for_page(title):
        return api_session.revisions.query(properties=rv_props.values(),
                                           titles={title},
                                           direction="newer")

    tsv_fields = ['title', 'pageid', 'namespace']
    tsv_fields = tsv_fields + list(rv_props.keys())

    # drop fields that we identified for exclusion
    tsv_fields = [e for e in tsv_fields if e not in exclude_from_tsv]

    # add special export fields
    tsv_fields = tsv_fields + ['url', 'export_timestamp', 'export_commit']

    export_info = { 'git_commit' : export_git_hash,
                    'timestamp' : export_time }

    with open(json_output_filename, 'w') as json_output, \
         open(tsv_output_filename, 'w') as tsv_output:

        tsv_writer = DictWriter(tsv_output, fieldnames=tsv_fields, delimiter="\t")
        tsv_writer.writeheader()

        for article in article_list:
            logging.info(f"pulling revisions for: {article}")
            for rev in get_revisions_for_page(article):
                logging.debug(f"processing raw revision: {rev}")

                # add export metadata
                rev['exported'] = export_info

                # save the json version of the code
                print(json.dumps(rev), file=json_output)

                # handle missing data
                if "sha1" not in rev:
                    rev["sha1"] = ""

                # add page title information
                rev['title'] = rev['page']['title']
                rev['pageid'] = rev['page']['pageid']
                rev['namespace'] = rev['page']['ns']

                # construct a URL
                rev['url'] = Request('GET', 'https://en.wikipedia.org/w/index.php',
                                     params={'title' : rev['title'].replace(" ", "_"),
                                            'oldid' : rev['revid']}).prepare().url

                rev['export_timestamp'] = export_time
                rev['export_commit'] = export_git_short_hash

                tsv_writer.writerow({k: rev[k] for k in tsv_fields})
            break

if __name__ == "__main__":

    main()
