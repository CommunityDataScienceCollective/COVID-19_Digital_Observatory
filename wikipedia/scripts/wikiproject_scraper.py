#!/usr/bin/env python3

###############################################################################
#
# This script scrapes the Covid-19 Wikiproject
# 
# It (1) hits the fcgi to find out how many rounds. Then (2) hit the fcgi 
# that many rounds, cooking that information down to just a list of article names and
# then (3) saves it out.
#
# At time of writing:
# the fCGI returns only 1000 max, no matter what you put in the limit. page 1 looks like this....
# https://tools.wmflabs.org/enwp10/cgi-bin/list2.fcgi?run=yes&projecta=COVID-19&namespace=&pagename=&quality=&importance=&score=&limit=1000&offset=1&sorta=Importance&sortb=Quality
#
# and page 2 looks like this
# https://tools.wmflabs.org/enwp10/cgi-bin/list2.fcgi?namespace=&run=yes&projecta=COVID-19&score=&sorta=Importance&importance=&limit=1000&pagename=&quality=&sortb=Quality&&offset=1001
#
###############################################################################

import argparse
import subprocess
import requests
import datetime
import logging
import re
import math
from bs4 import BeautifulSoup

def parse_args():

    parser = argparse.ArgumentParser(description='Get a list of pages tracked by the COVID-19 Wikiproject.')
    parser.add_argument('-o', '--output_file', help='Where to save output', default="wikipedia/resources/enwp_wikiproject_covid19_articles.txt", type=str)
    parser.add_argument('-L', '--logging_level', help='Logging level. Options are debug, info, warning, error, critical. Default: info.', default='info', type=digobs.get_loglevel), 
    parser.add_argument('-W', '--logging_destination', help='Logging destination file. (default: standard error)', type=str), 
    args = parser.parse_args()

    return(args)

def main():

    args = parse_args()
    outputFile = args.output_file

    #handle -W
    if args.logging_destination:
        logging.basicConfig(filename=args.logging_destination, filemode='a', level=args.logging_level)
    else:
        logging.basicConfig(level=args.logging_level)

    export_git_hash = subprocess.check_output(['git', 'rev-parse', 'HEAD']).decode().strip()
    export_git_short_hash = subprocess.check_output(['git', 'rev-parse', '--short', 'HEAD']).decode().strip()
    export_time = str(datetime.datetime.now())

    logging.info(f"Starting at {export_time} and destructively outputting article list to {outputFile}.")
    logging.info(f"Last commit: {export_git_hash}")

    #1 How many hits to the fcgi?
    session = requests.Session()

    originalURL = "https://tools.wmflabs.org/enwp10/cgi-bin/list2.fcgi?run=yes&projecta=COVID-19&namespace=&pagename=&quality=&importance=&score=&limit=1000&offset=1&sorta=Importance&sortb=Quality"
    headURL = "https://tools.wmflabs.org/enwp10/cgi-bin/list2.fcgi?run=yes&projecta=COVID-19&namespace=&pagename=&quality=&importance=&score=&limit=1000&offset=" 
    tailURL = "&sorta=Importance&sortb=Quality" #head + offset + tail = original when offset = 1

    # find out how many results we have
    response = session.get(originalURL)

    soup = BeautifulSoup(response.text, features="html.parser")
    nodes = soup.find_all('div', class_="navbox")
    rx = re.compile("Total results:\D*(\d+)") 
    m = rx.search(nodes[0].get_text())
    #print(nodes[0].get_text())
    numResults = int(m.group(1))

    logging.debug(f"fcgi returned {numResults}")
    rounds = math.ceil(numResults/1000) 

    #2 Fetch and parse down to just the article names
    articleNames = []

    for i in range(1, rounds+1):
        offset = (i - 1)*1000 + 1 #offset is 1, then 1001, then 2001 
        url = f"{headURL}{offset}{tailURL}"
        response = session.get(url)
        soup = BeautifulSoup(response.text, features="html.parser") #make fresh soup
        article_rows = soup.find_all('tr', class_="list-odd") #just the odds first
        for row in article_rows:
            a = row.find('a')
            articleNames.append(a.get_text())
        article_rows = soup.find_all('tr', class_="list-even") #now the events
        for row in article_rows:
            a = row.find('a')
            articleNames.append(a.get_text())

    #3 Saves the list to a file

    with open(outputFile, 'w') as f:
        f.write('\n'.join(articleNames)+'\n')
    logging.debug(f"Finished scrape and made a new article file at {datetime.datetime.now()}")


if __name__ == "__main__":

    main()

