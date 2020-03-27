#!/usr/bin/env python3

###############################################################################
#
# This script assumes the presence of the COVID-19 repo.
# 
# It (1) reads in the article list and then (2) calls the Wikimedia API to 
# fetch view information for each article. Output is to a (3) TSV file.
#
#
###############################################################################


#1 Load up the list of article names

#2 Repeatedly call the API with that list of names

#3 Save results as a TSV

import requests
import argparse
import json
import csv
import time
import os.path
import datetime



def parse_args():

    parser = argparse.ArgumentParser(description='Call the views API repeatedly.')
    parser.add_argument('-o', '--output_folder', help='Where to save output', default="", type=str)
    parser.add_argument('-i', '--article_file', help='File listing article names', default="../resources/articles.txt", type=str)
    parser.add_argument('-d', '--query_date', help='Date if not yesterday, in YYYYMMDD format please.', type=str)
    args = parser.parse_args()

    return(args)


def main():

    args = parse_args()

    outputPath = args.output_folder
    articleFile = args.article_file
    if (query_date):
        queryDate = args.query_date
    else:
        queryDate = datetime.datetime.today().strftime("%Y%m%d")


    with open(articleFile, 'r') as infileHandle:
        theInfile = csv.reader(infileHandle, quotechar='"')
        for currentLine in theInfile:
            articleList.append(currentLine["Article"])

    with open(outputPath, 'w') as outfile:
        outfile.write("[")

    i = 0 #iterator to deal with end of file

    for a in articleList:
        i = i+1
        url= "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/en.wikipedia/all-access/all-agents/"
        url= url + a + "/daily/" + queryDate + "/" + queryDate #for now, single date at a time


        response = requests.get(url)
        if raw_response.ok:
            with open(outputPath, 'a') as outfile:
                json.dump(json.loads(ident_response.content), outfile)
                if i < len(revList):
                    outfile.write(",\n")
                else: #at end of file
                    outfile.write("\n")

            time.sleep(1)


    with open(outputPath, 'a') as outfile:
        outfile.write("]")



if __name__ == "__main__":

    main()
