#!/usr/bin/env python
# coding: utf-8

import pathlib
import json
import re
import requests
import time
import config
import argparse
import logging
import urllib.parse
import csv
import datetime
from urllib.error import HTTPError


ENDPT = 'https://web.archive.org/save/'
UA_STRING = config.UA_STRING
ACCESS_KEY = config.ACCESS_KEY
SECRET_KEY = config.SECRET_KEY
HEADERS = {'Accept':'application/json',
           'User-Agent': UA_STRING,
           'Authorization': f'LOW {ACCESS_KEY}:{SECRET_KEY}'}
IF_NOT_ARCHIVED_WITHIN = '20h' # If an archive has been made in this long, don't make another one
CHUNK_SIZE = 25 # Get this many URLS at a time
QUERY_OUTLINKS = 0 # Whether to capture outlinks for query URLS (1 = yes, 0 = no)
# Setting this to 0 for now, since Google is giving 429s.



def main():
    parser = argparse.ArgumentParser(description='Creates job ids from to_archive csv file')
    parser.add_argument('-i', help='Location of to_archive CSV file')
    parser.add_argument('-o', help='Location to save wayback URL file')
    args = parser.parse_args()

    print(f"Starting at {datetime.datetime.now()}")

    def get_job_ids(urls, capture_outlinks):
        for url in urls:
            if url not in completed_urls:
                job_id = archive_url(url,
                        capture_outlinks = capture_outlinks)
                # Just put them into the job_id_tuples
                job_id_tuples.append((url, job_id))
            else:
                logging.debug(f'{url} was in completed')



    def get_wayback_urls(out_file):
        for url, job_id in job_id_tuples:
            try:
                wb_url, timestamp = get_wayback_url(job_id)
                write_wayback(out_file, url, wb_url, timestamp)
            except ConnectionError:
                logging.warning(f'{url} with job id {job_id} failed with a ConnectionError')
            except TypeError:
                logging.warning(f'{url} with job id {job_id} did not get a WB URL')
            except HTTPError as e:
                logging.warning(f'{url} with job id {job_id} failed with an uncaught HTTP Error: {e}')
            except Exception as e:
                logging.warning(f'{url} with job id {job_id} failed with an uncaught Exception: {e}')


    completed_urls = get_completed(args.o, time_string = IF_NOT_ARCHIVED_WITHIN)
    to_archive = load_urls(args.i)
    # Do query URLS first, since for them we'll capture outlinks
    query_urls = [x for x in to_archive if to_archive[x] == 'query']
    link_urls = [x for x in to_archive if to_archive[x] == 'link']

    with open(args.o, 'a') as out_file:
        out = csv.writer(out_file)
        logging.info("Now retrieving query urls")
        for q_chunk in chunk_list(query_urls, CHUNK_SIZE):
            job_id_tuples = [] # Stores which urls need to be retrieved (populated by get_job_ids)
            get_job_ids(q_chunk, capture_outlinks = QUERY_OUTLINKS)
            get_wayback_urls(out)

        logging.info("Now retrieving link urls")
        for chunk in chunk_list(link_urls, CHUNK_SIZE):
            job_id_tuples = []
            get_job_ids(chunk, capture_outlinks = 0)
            get_wayback_urls(out)

    print(f"Ending at {datetime.datetime.now()}")

def chunk_list(l, size):
    for i in range(0, len(l), size):
        logging.info(f'Now getting items {i} through {min(len(l), i + size)} of {len(l)}')
        yield l[i:i+size]

def load_urls(url_fn):
    result = {}
    with open(url_fn, 'r') as fn:
        f = csv.reader(fn)
        for row in f:
            result[row[0]] = row[1]
    return result


def write_wayback(f, url, wayback_url, timestamp):
    '''Takes a CSV writer object, a url, and wayback_url, and writes
    it out'''
    f.writerow([timestamp,url,wayback_url])


def get_completed(csv_file, time_string):
    '''Loads all of the completed URLs from the csv file. Takes in a time string like '20h',
    strips the last character, and assumes that it refers to the number of hours.
    Does not load any URLs older than that.
    '''
    delta_hours = int(time_string[:-1])
    result = {}
    if pathlib.Path(csv_file).exists():
        with open(csv_file, 'r') as fn:
            f = csv.reader(fn)
            for row in f:
                dt = datetime.datetime.strptime(row[0], '%Y%m%d%H%M%S')
                if datetime.datetime.now() - dt > datetime.timedelta(hours = delta_hours):
                    continue
                else:
                    result[row[1]] = row[2]
    return result

def urlencode_url(url):
    return requests.utils.requote_uri(urllib.parse.unquote_plus(url))

def archive_url(url,
                wait = 6,
                capture_outlinks = 0 # Whether to capture outlinks (default is no)
                ):

    logging.debug(f'Sending archive call for {url}')
    payload = {'url': url,
              'if_not_archived_within' : IF_NOT_ARCHIVED_WITHIN,
              #'capture_screenshot': capture_screenshot,
              'capture_outlinks': capture_outlinks
              }
    r = requests.post(ENDPT, headers=HEADERS, data=payload)
    logging.debug(r.content)

    if r.status_code == 429:
        logging.debug(f'Hit rate limit, now waiting for {wait:.2f} seconds')
        time.sleep(wait)
        return archive_url(url = url,
                           wait = wait * 1.2, 
                           capture_outlinks = capture_outlinks)
    if r.status_code in [104,401,404,443,502,503,504]:
        logging.warning(url)
        logging.warning(r.text)
        if r.status_code in [104, 401, 443]:
            logging.warning(f'104, 401, or 443 received when archiving {url}. Giving up.')
            return None
        logging.warning('502 or 503 or 504 status received; waiting 30 seconds')
        time.sleep(30)
        return archive_url(url = url,
                           capture_outlinks = capture_outlinks)

    r.raise_for_status()
    try:
        return r.json()['job_id']
    except KeyError:
        logging.warning(f'Should have a valid job id for {url}. Instead, this was returned:\n {r.content}')


def get_wayback_url(job_id):

    def call_status_url(job_id,
                         wait = 6, # Initial wait time
                         max_wait = 12 # Stop when wait time between calls hits max_wait
                        ):
        '''Helper function to handle the call to the status API'''
        if job_id is None:
            return None
        s = requests.get(ENDPT + 'status/' + job_id, headers=HEADERS)
        if s.status_code == 200:
            s_json = s.json()
            if s_json['status'] == 'pending':
                if wait > max_wait:
                    logging.debug(s_json)
                    logging.warning(f"The call to get the status of job id {job_id} failed. Skipping")
                    return None
                logging.debug(f'Pending, now waiting for {wait:.2f} seconds')
                time.sleep(wait)
                return call_status_url(job_id, wait = wait + 1)
            if s_json['status'] == 'success':
                return s_json
            if s_json['status'] == 'error':
                # Sometimes we get 429s from Google; if that's the case, then wait 30 seconds before trying again
                if re.search('HTTP status=429', s_json['message']):
                    url = re.search('http\S*', s_json['message'])
                    if url:
                        url = url.group()
                        logging.warning(f"Wayback received a 429 when trying to archive {url}. Skipping this one and waiting 30 seconds before trying again")
                        time.sleep(30)
                        return None
                    else:
                        return None
                logging.error('Could not get status, with error: {}'.format(s_json["message"]))
                return None
            else:
                logging.warning(s_json)
                raise ValueError("Status was unexpected")
        if s.status_code == 429:
            logging.debug(f'Hit rate limit, now waiting for {wait} seconds')
            time.sleep(wait)
            return call_status_url(job_id, wait = wait * 1.2) # Backoff
        if s.status_code in [104,401,404,443,502,503,504]:
        # These likely mean something's wrong; wait and then try again
            if s.status_code in [104, 401, 443]:
                logging.warning(f'104, 401, or 443 received when archiving {url}. Giving up.')
                return None
            logging.warning('443, 502, 503, or 504 status received; waiting 30 seconds')
            logging.warning(s.text)
            time.sleep(30)
            return call_status_url(job_id)
        else:
            s.raise_for_status()

    logging.debug(f"Getting wayback URL for job id {job_id}")
    s_json = call_status_url(job_id)
    if s_json is None:
        return s_json
    try:
        wayback_url = 'http://web.archive.org/web/{}/{}'.format(s_json['timestamp'],
                                                         s_json['original_url'])
        return (wayback_url, s_json['timestamp'])
    except KeyError:
        logging.error(f"Missing timestamp or original URL for {job_id}")
        return None

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
