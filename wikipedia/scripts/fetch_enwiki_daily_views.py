#!/usr/bin/env python3

###############################################################################
#
# This script assumes the presence of the COVID-19 repo.
# 
# It (1) reads in the article list and then (2) calls the Wikimedia API to 
# fetch view information for each article. Output is to (3) JSON and TSV.
#
###############################################################################

import sys
import requests
import argparse
import json
import time
import os.path
import datetime
import logging
from csv import DictWriter
import digobs
#import feather #TBD

def parse_args():
    parser = argparse.ArgumentParser(description='Call the views API to collect Wikipedia view data.')
    parser.add_argument('-o', '--output_folder', help='Where to save output', default="wikipedia/data", type=str)
    parser.add_argument('-i', '--article_file', help='File listing article names', default="wikipedia/resources/enwp_wikiproject_covid19_articles.txt", type=str)
    parser.add_argument('-d', '--query_date', help='Date if not yesterday, in YYYYMMDD format.', type=str)
    parser.add_argument('-L', '--logging_level', help='Logging level. Options are debug, info, warning, error, critical. Default: info.', default='info', type=digobs.get_loglevel), 
    parser.add_argument('-W', '--logging_destination', help='Logging destination file. (default: standard error)', type=str), 
    args = parser.parse_args()
    return(args)

def main():

    args = parse_args()

    outputPath = args.output_folder
    articleFile = args.article_file

    #handle -d
    if args.query_date:
        query_date = args.query_date
    else:
        yesterday = datetime.datetime.today() - datetime.timedelta(days=1)
        query_date = yesterday.strftime("%Y%m%d")

    #handle -W
    if args.logging_destination:
        logging.basicConfig(filename=args.logging_destination, filemode='a', level=args.logging_level)
    else:
        logging.basicConfig(level=args.logging_level)

    export_time = str(datetime.datetime.now())
    export_date = datetime.datetime.today().strftime("%Y%m%d")

    logging.info(f"Starting run at {export_time}")
    logging.info(f"Last commit: {digobs.git_hash()}")

    #1 Load up the list of article names
    j_outfilename = os.path.join(outputPath, f"digobs_covid19-wikipedia-enwiki_dailyviews-{query_date}.json")
    t_outfilename = os.path.join(outputPath, f"digobs_covid19-wikipedia-enwiki_dailyviews-{query_date}.tsv")

    with open(articleFile, 'r') as infile:
        articleList = list(map(str.strip, infile))

    success = 0 #for logging how many work/fail
    failure = 0 

    #3 Save results as a JSON and TSV
    with open(j_outfilename, 'w') as j_outfile, \
         open(t_outfilename, 'w') as t_outfile:

        #2 Repeatedly call the API with that list of names
        for a in articleList:
            url= f"https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/en.wikipedia/all-access/all-agents/{a}/daily/{query_date}00/{query_date}00"
           
            # try to grab the code 10 times, sleeping for one minute each time
            tries = 0
            while True:
                try:
                    response = requests.get(url)
                    logging.debug(f"Successfully requested data from: {url}")
                    break
                except:
                    if tries > 10:
                        logging.critical(f"Error: giving up after 10 tries to get {url}")
                        raise
                    else:
                        logging.warning(f"Warning: socket.timeout from {url}")
                        logging.warning(f"Warning: sleeping 60 seconds before retrying")
                        tries = tries + 1
                        time.sleep(60)
                        continue
            
            if response.ok:
                jd = response.json()["items"][0]
                success = success + 1
            else:
                failure = failure + 1
                logging.warning(f"Failure: {response.status_code} from {url}")
                continue

            # start writing the CSV File if it doesn't exist yet
            try:
                dw
            except NameError:
                dw = DictWriter(t_outfile, sorted(jd.keys()), delimiter='\t')
                dw.writeheader()

            logging.debug(f"printing data: {jd}")

            # write out the line of the json file
            print(json.dumps(jd), file=j_outfile)

            # write out of the csv file
            dw.writerow(jd)

    # f_Out = outputPath + "dailyviews" + query_date + ".feather"
    # read the json back in and make a feather file? 
    logging.debug(f"Run complete at {datetime.datetime.now()}")
    logging.info(f"Processed {success} successful URLs and {failure} failures.")


if __name__ == "__main__":

    main()
