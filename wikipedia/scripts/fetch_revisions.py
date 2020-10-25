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
from os import path, mkdir
import json
import datetime
import sqlite3
from functools import partial
from itertools import chain
from csv import DictWriter
import digobs

def main():

    parser = argparse.ArgumentParser(description='Call the views API to collect Wikipedia revision data.')
    parser.add_argument('-o', '--output_folder', help='Where to save output', default="wikipedia/data", type=str)
    parser.add_argument('-i', '--input_file', help="Input a file of page names from the English Wikiproject.", type=argparse.FileType('r'), default='./wikipedia/resources/enwp_wikiproject_covid19_articles.txt')
    parser.add_argument('-d', '--input_db', help="Input a path to a sqlite3 database from the real-time-covid-tracker project", type = sqlite3.connect, default='real-time-wiki-covid-tracker/AllWikidataItems.sqlite')
    parser.add_argument('-L', '--logging_level', help='Logging level. Options are debug, info, warning, error, critical. Default: info.', default='info', type=digobs.get_loglevel), 
    parser.add_argument('-W', '--logging_destination', help='Logging destination file. (default: standard error)', type=argparse.FileType('a'))

    args = parser.parse_args()

    logging = digobs.init_logging(args)

    conn = args.input_db
    conn.row_factory = sqlite3.Row

    projects = (row['project'] for row in conn.execute("SELECT DISTINCT project from pagesPerProjectTable;").fetchall())

    tsv_fields = ['title', 'pageid', 'namespace']

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
    
    def get_project_pages(project):
        return (row['page'] for row in conn.execute(f"SELECT DISTINCT page FROM pagesPerProjectTable WHERE project == '{project}';").fetchall())

    def get_project_revisions(project):
        pages = get_project_pages(project)
        if project=="en.wikipedia":
            pages = chain(pages, map(str.strip,args.input_file))
        return digobs.get_pages_revisions(pages, project=project, logging=logging, rv_props=rv_props)

    tsv_fields = tsv_fields + list(rv_props.keys())

    exclude_from_tsv = ['tags', 'comment', 'content', 'flags']

    # drop fields that we identified for exclusion
    tsv_fields = [e for e in tsv_fields if e not in exclude_from_tsv]

    # add special export fields
    tsv_fields = tsv_fields + ['anon', 'minor', 'url', 'export_timestamp', 'export_commit']
    
    export_time = str(datetime.datetime.now())

    rev_batch_to_tsv = partial(digobs.rev_batch_to_tsv,
                         tsv_fields = tsv_fields,
                         export_info={'export_timestamp':export_time,
                                      'export_commit':digobs.git_hash(short=True)})    

    export_info = { 'git_commit' : digobs.git_hash(),
                    'timestamp' : export_time }

    export_date = datetime.datetime.today().strftime("%Y%m%d")

    rev_batch_to_json = partial(digobs.rev_batch_to_json,
                                export_info = export_info)

    def write_project_pages(project):
        project_folder = path.join(args.output_folder, project)
        if not path.exists(project_folder):
            mkdir(project_folder)

        dump_folder = path.join(project_folder, export_date)
        if not path.exists(dump_folder):
            mkdir(dump_folder)

        project_revs = get_project_revisions(project)
        
        json_output_filename = path.join(dump_folder, f"digobs_covid19_{project}_revisions-{export_date}.json")
        tsv_output_filename =  path.join(dump_folder, f"digobs_covid19_{project}_revisions-{export_date}.tsv")

        with open(json_output_filename, 'w') as json_output, \
             open(tsv_output_filename, 'w') as tsv_output:
            tsv_writer = DictWriter(tsv_output, fieldnames=tsv_fields, delimiter="\t")
            tsv_writer.writeheader()
  
            for rev_batch in project_revs:
                logging.debug(f"processing raw revision: {rev_batch}")
                rev_batch_to_json(rev_batch, json_output=json_output)
                rev_batch_to_tsv(rev_batch, project=project, tsv_writer=tsv_writer)

    for project in projects:
        write_project_pages(project)

if __name__ == "__main__":
    main()
