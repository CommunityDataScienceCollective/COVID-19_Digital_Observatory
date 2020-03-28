#!/usr/bin/env python3

###############################################################################
#
# This script assumes the presence of the COVID-19 repo.
# 
# It (1) reads in the article list and then (2) calls the Wikimedia API to 
# fetch view information for each article. Output is to a (3) JSON, TSV, and 
# Feather file.
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
    parser.add_argument('-o', '--output_folder', help='Where to save output', default="../data/", type=str)
    parser.add_argument('-i', '--article_file', help='File listing article names', default="../resources/articles.txt", type=str)
    parser.add_argument('-d', '--query_date', help='Date if not yesterday, in YYYYMMDD format please.', type=str)
    args = parser.parse_args()

    return(args)


def main():

    args = parse_args()

    outputPath = args.output_folder
    articleFile = args.article_file

    if (args.query_date):
        queryDate = args.query_date
    else:
        yesterday = datetime.datetime.today() - datetime.timedelta(days=1)
        queryDate = yesterday.strftime("%Y%m%d")

    queryDate = queryDate + "00" #requires specifying hours


    articleList = []
    with open(articleFile, 'r') as infileHandle:
        #theInfile = csv.reader(infileHandle, quotechar='"')
        theInfile = csv.reader(infileHandle)
        next(theInfile) #skip header
        for currentLine in theInfile:
            articleList.append(currentLine)

    j_Out = outputPath + "dailyviews" + queryDate + ".json"
    with open(j_Out, 'w') as outfile:
        outfile.write("[")

    i = 0 #iterator to deal with end of file

    for a in articleList:
        a = a[0] #destringify
        i = i+1
        url= "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/en.wikipedia/all-access/all-agents/"
        url= url + a + "/daily/" + queryDate + "/" + queryDate #for now, single date at a time


        response = requests.get(url)
        if response.ok:

            #do json entry
            j=json.loads(response.content)
            with open(j_Out, 'a') as j_outfile: 
                json.dump(j, j_outfile)
                if i < len(articleList):
                    j_outfile.write(",\n")
                else: #at end of file
                    j_outfile.write("\n")

            #do tsv entry
            #with open(outputPath + "dailyviews" + queryDate + ".tsv", 'a') as t_outfile: 
            #    dw = csv.DictWriter(t_outfile, sorted(j[0].keys()), delimiter='\t')
            #    if i==1:
            #        dw.writeheader()
            #    dw.writerows(j)

            time.sleep(.1)

    with open(j_Out, 'a') as j_outfile:
        j_outfile.write("]")

    #read the json back in and make a feather file?



if __name__ == "__main__":

    main()
