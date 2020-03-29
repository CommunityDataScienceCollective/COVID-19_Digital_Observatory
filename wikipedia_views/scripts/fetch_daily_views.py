#!/usr/bin/env python3

###############################################################################
#
# This script assumes the presence of the COVID-19 repo.
# 
# It (1) reads in the article list and then (2) calls the Wikimedia API to 
# fetch view information for each article. Output is to (3) JSON and TSV.
#
###############################################################################


import requests
import argparse
import json
import csv
import time
import os.path
import datetime
import logging
#import feather #TBD


def parse_args():

    parser = argparse.ArgumentParser(description='Call the views API repeatedly.')
    parser.add_argument('-o', '--output_folder', help='Where to save output', default="../data/", type=str)
    parser.add_argument('-i', '--article_file', help='File listing article names', default="../resources/articles.txt", type=str)
    parser.add_argument('-d', '--query_date', help='Date if not yesterday, in YYYYMMDD format please.', type=str)
    parser.add_argument('-L', '--logging_level', help='Logging level. Options are debug, info, warning, error, critical. Default: info.', default='info'), 
    parser.add_argument('-W', '--logging_destination', help='Logging destination.', default='../logs/'), 
    args = parser.parse_args()

    return(args)


def main():

    args = parse_args()

    outputPath = args.output_folder
    articleFile = args.article_file

    #handle -d
    if (args.query_date):
        queryDate = args.query_date
    else:
        yesterday = datetime.datetime.today() - datetime.timedelta(days=1)
        queryDate = yesterday.strftime("%Y%m%d")

    queryDate = queryDate + "00" #requires specifying hours

    #handle -W
    logHome = f"{args.logging_destination}dailylogrun{datetime.datetime.today().strftime('%Y%m%d')}"

    #handle -L
    loglevel = args.logging_level
    if loglevel == 'debug':
        logging.basicConfig(filename=logHome, filemode='a', level=logging.DEBUG)
    elif loglevel == 'info':
        logging.basicConfig(filename=logHome, filemode='a', level=logging.INFO)
    elif loglevel == 'warning':
        logging.basicConfig(filename=logHome, filemode='a', level=logging.WARNING)
    elif loglevel == 'error':
        logging.basicConfig(filename=logHome, filemode='a', level=logging.ERROR)
    elif loglevel == 'critical':
        logging.basicConfig(filename=logHome, filemode='a', level=logging.CRITICAL)
    else: 
        print("Choose a valid log level: debug, info, warning, error, or critical") 
        exit


    articleList = []
    logging.debug(f"Starting run at {datetime.datetime.now()}")

    #1 Load up the list of article names

    j_Out = f"{outputPath}dailyviews{queryDate}.json"
    t_Out = f"{outputPath}dailyviews{queryDate}.tsv"

    with open(articleFile, 'r') as infile:
        next(infile) #skip header
        articleList = list(infile)

    j = []
    success = 0 #for logging how many work/fail
    failure = 0 

    #2 Repeatedly call the API with that list of names

    for a in articleList:
        a = a.strip("\"\n") #destringify
        url= f"https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/en.wikipedia/all-access/all-agents/{a}/daily/{queryDate}/{queryDate}"

        response = requests.get(url)
        if response.ok:
            jd = json.loads(response.content)
            j.append(jd["items"][0])
            time.sleep(.1)
            success = success + 1
        else:
            failure = failure + 1
            logging.warning(f"Failure: {response.status_code} from {url}")

    #3 Save results as a JSON and TSV

    #all data in j now, make json file
    logging.info(f"Processed {success} successful URLs and {failure} failures.")

    with open(j_Out, 'w') as j_outfile: 
        json.dump(j, j_outfile, indent=2)

    with open(t_Out, 'w') as t_outfile:
        dw = csv.DictWriter(t_outfile, sorted(j[0].keys()), delimiter='\t')
        dw.writeheader()
        dw.writerows(j)

    logging.debug(f"Run complete at {datetime.datetime.now()}")

    # f_Out = outputPath + "dailyviews" + queryDate + ".feather"
    # read the json back in and make a feather file? 


if __name__ == "__main__":

    main()
