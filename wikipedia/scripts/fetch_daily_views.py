#!/usr/bin/env python3
import argparse
import sqlite3
import requests
from datetime import datetime, timedelta
import logging
import digobs
from os import path, mkdir
from functools import partial
from itertools import chain

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Get a list of pages related to COVID19, pandemic, and SARS-COV2 virus related entities.")
    parser.add_argument('-o', '--output_folder', help='Where to save output', default="wikipedia/data", type=str)
    parser.add_argument('-L', '--logging_level', help='Logging level. Options are debug, info, warning, error, critical. Default: info.', default='info', type=digobs.get_loglevel)
    parser.add_argument('-W', '--logging_destination', help='Logging destination file. (default: standard error)', type=argparse.FileType('a'))
    parser.add_argument('-d', '--query_date', help='Date if not yesterday, in YYYYMMDD format.', type=lambda s: datetime.strptime(s, "%Y%m%d"))

    parser.add_argument('-i', '--input_file', help="Input a file of page names from the English Wikiproject.", type=argparse.FileType('r'), default='./wikipedia/resources/enwp_wikiproject_covid19_articles.txt')

    parser.add_argument('-b', '--input_db', help="Input a path to a sqlite3 database from the real-time-covid-tracker project", type = sqlite3.connect, default='real-time-wiki-covid-tracker/AllWikidataItems.sqlite')

    args = parser.parse_args()
    conn = args.input_db
    conn.row_factory = sqlite3.Row

    #handle -d
    if args.query_date:
        query_date = args.query_date.strftime("%Y%m%d")
    else:
        yesterday = datetime.today() - timedelta(days=1)
        query_date = yesterday.strftime("%Y%m%d")

    digobs.init_logging(args)

    logging.info(f"Destructively outputting results to {args.output_folder}")

    #1 Load up the list of article names

    logging.info("loading info from database")
    projects = [row['project'] for row in conn.execute("SELECT DISTINCT project from pagesPerProjectTable;").fetchall()]

    successes = 0
    failures = 0

    for project in projects:
        project_folder = path.join(args.output_folder, project)
        if not path.exists(project.folder):
            mkdir(project_folder)

        dump_folder = path.join(projct.folder, export_date)
        if not path.exists(dump_folder):
            mkdir(dump_folder)

        logging.info(f"Getting page views for {project}")
        rows = conn.execute(f"SELECT DISTINCT page from pagesPerProjectTable WHERE project='{project}';").fetchall()
        pages = (row['page'] for row in rows)

        # special case for english, we have a wikiproject input file
        if project == "en.wikipedia":
            pages = chain(pages, map(str.strip,args.input_file))

        call_view_api = partial(digobs.call_view_api, project=project, query_date = query_date)

        responses = map(call_view_api, pages)
        
        j_outfilename = path.join(j_output_folder, f"digobs_covid19_{project}_dailyviews-{query_date}.json")
        t_outfilename = path.join(t_output_folder, f"digobs_covid19_{project}_dailyviews-{query_date}.tsv")

        with open(j_outfilename, 'w') as j_outfile, \
             open(t_outfilename, 'w') as t_outfile:

            proj_successes, proj_failures = digobs.process_view_responses(responses, j_outfile, t_outfile, logging)
        logging.info(f"(Processed {proj_successes} successes and {proj_failures} for {project}")
        successes = proj_successes + successes
        failures = proj_failures + failures

    conn.close()
    # f_Out = outputPath + "dailyviews" + query_date + ".feather"
    # read the json back in and make a feather file? 
    logging.debug(f"Run complete at {datetime.now()}")
    logging.info(f"Processed {successes} successful URLs and {failures} failures.")

        
