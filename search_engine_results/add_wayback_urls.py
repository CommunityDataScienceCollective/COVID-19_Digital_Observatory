#!/usr/bin/env python
# coding: utf-8

import pathlib
import pprint
import json
import re
import requests
import time
import config
import argparse
from collections import Counter
import logging
import urllib.parse
import csv


ENDPT = 'https://web.archive.org/save/'
UA_STRING = config.UA_STRING
ACCESS_KEY = config.ACCESS_KEY
SECRET_KEY = config.SECRET_KEY
HEADERS = {'Accept':'application/json',
           'User-Agent': UA_STRING,
           'Authorization': f'LOW {ACCESS_KEY}:{SECRET_KEY}'}
IF_NOT_ARCHIVED_WITHIN = '20h' # If an archive has been made in this long, don't make another one


########
# The goal of this program is to take all of the URLs from SERPs and archive them in the Wayback Machine,
# and then store the Wayback URLs as part of the SERP metadata.
#
# There is a lot of overlap in URLs so we store previous results in temporary files so we don't repeat the same calls
#
# Example usage: 
# python3 add_wayback_urls.py -i /path/to/serps/dir -t ./tmp --ignore_self_links # Note that this would overwrite the json files in the /path/to/serps/dir directories
########


def main():
    parser = argparse.ArgumentParser(description='Add wayback URLs to SERP metadata.')
    parser.add_argument('-i', help='Input directory with metadata files')
    parser.add_argument('-o', help='Location to save modified files (if blank, will overwrite)')
    ## TODO: Maybe switch this so default is to ignore?
    parser.add_argument('--ignore_self_links', help='Whether to ignore links from the same domain as the query',
            action='store_true')
    parser.add_argument('-t', help='Temp directory location (to save/load job ids, outlinks, and wayback URLs)')

    args = parser.parse_args()

    
    # Make a list of the files that we are going to be editing (skip those already edited)
    files = pathlib.Path(args.i).glob('**/*.json')
    ## FOR TESTING ONLY!!!
    #files = list(files)[10:11]
    incomplete_files = list(files)
    while len(incomplete_files) > 0:
        for fn in incomplete_files:
            try:
                add_wayback_urls(fn, args.o, args.t, args.ignore_self_links)
                incomplete_files.pop(incomplete_files.index(fn)) # if it works, remove it from the list
            except ConnectionError:
                failed_files.append(fn)


def add_wayback_urls(filename, out_dir, temp_dir, ignore_self_links = False, remove_cache = True):

    def output_file_exists(f):
        '''Says whether the output file for a given file exists, so that we can skip it if it's done.
        Assumes that if the input file and the output file are the same, we want to overwrite the input file.
        We therefore return False in this case'''
        out_path = get_out_path(f, out_dir)
        if out_path == f:
            return False
        return out_path.exists()


    def write_wayback_to_file(filename, temp_file):
        url_to_wb = {}
        with open(filename, 'r') as f:
            with open(temp_file, 'a') as tf:
                tf_csv = csv.writer(tf)
                j_obj = json.load(f)
                query_url = j_obj['link']
                try:
                    wayback_url = wayback_dict[query_url].get_wayback_url()
                    j_obj['wayback_url'] = wayback_url
                    tf_csv.writerow([query_url, wayback_url])
                except KeyError:
                    logging.error(f"Should have an entry for {query_url}")
                    logging.error(wayback_dict.keys())
                    j_obj['wayback_url'] = ''
                for link_obj in j_obj['linkElements']:
                    link_url = link_obj['href']
                    try:
                        wayback_url = wayback_dict[link_url].get_wayback_url()
                        link_obj['wayback_url'] = wayback_url
                        tf_csv.writerow([link_url, wayback_url])
                    except KeyError:
                        if link_url in urls_to_archive:
                            # If it's in the urls to archive, then it should be in the dictionary.
                            logging.error(f"Should have an entry for {link_url}")
                            logging.error(wayback_dict.keys())
                            logging.error(link_obj['href'])
                        link_obj['wayback_url'] = ''
        outfile = get_out_path(filename, out_dir)
        with open(outfile, 'w') as f:
            json.dump(j_obj, f)

    tmp_dir =  pathlib.Path(temp_dir)
    if not tmp_dir.exists():
        tmp_dir.mkdir()

    # Query urls are the SERP query URLs; we get all of the outgoing links for these, to
    # hopefully avoid duplication and having too many active jobs
    query_urls =[]
    urls_to_archive = []
    urls_to_skip = get_skipped_urls(temp_dir) # Skip these unless they appear > once
    # First read the files and create a list of URLs to archive
    with open(filename, 'r') as f:
        j_obj = json.load(f)
        # If this file already has wayback info, or if the output file has been created,
        # the skip it
        if output_file_exists(filename) or 'wayback_url' in j_obj:
            return None
        query_url, other_urls = get_urls_from_json(j_obj)
        query_urls.append(query_url)
        # Remove self links
        if ignore_self_links:
            domain = get_domain(query_url)
        else:
            domain = None
        to_archive, to_skip = filter_urls(other_urls, domain, remove_cache)
        print(to_archive)
        print(to_skip)
        urls_to_archive += to_archive
        urls_to_skip += to_skip
    
    # For the URLs that we would otherwise skip, grab them if they occur
    # more than once. Write the rest back to the temp file to check next time
    with open(temp_dir + '/skipped_urls.csv', 'w') as tf:
        f = csv.writer(tf)
        for url, occurrences in Counter(urls_to_skip).items():
            if occurrences > 1:
                logging.info(f"{url} appears {occurrences} times. Adding to archive list")
                urls_to_archive.append(url)
            else:
                f.writerow([url])
    
    # Get the URLS from the wayback APIs
    wayback_dict = get_wayback_urls(query_urls, urls_to_archive, temp_dir)
    write_wayback_to_file(filename, temp_dir + '/wayback_urls.csv')


def get_skipped_urls(temp_dir):
    result = []
    if pathlib.Path(temp_dir + '/skipped_urls.csv').exists():
        with open(temp_dir + '/skipped_urls.csv', 'r') as f:
            for row in f:
                result.append(row)
    return result

def get_out_path(fp, out_dir):
    '''Assumes that we want to keep the directory and the file name'''
    if out_dir == None:
        return fp
    else:
        new_path = pathlib.Path(out_dir).joinpath(*fp.parts[-2:])
        if not new_path.parent.exists():
            new_path.parent.mkdir(parents = True)
        return new_path

def dict_from_temp(temp_file):
    result = {}
    if pathlib.Path(temp_file).exists():
        with open(temp_file, 'r') as fn:
            f = csv.reader(fn)
            for row in f:
                result[row[0]] = row[1]
    return result

def get_wayback_urls(query_urls, urls_to_archive, temp_dir):
    '''
    Takes in two lists of urls. The first are the URLs of the SERPS.
    For these, we add a flag to the API to create archives of all outgoing links.
    This should include many of the same links that we gathered, thus reducing the number
    of calls that we need to make.
    
    Returns a dictionary of URLs and job ids which can be used to get the
    archive.org URLs
    '''

    # Get job_ids and wayback_urls from the temp file
    job_ids = dict_from_temp(temp_dir + '/job_ids.csv' )
    wayback_urls = dict_from_temp(temp_dir + '/wayback_urls.csv' )

    # And save them as a class attributes
    URLObj.job_ids = job_ids
    URLObj.wayback_urls = wayback_urls

    # First, we need to get the job ids
    url_obj_dict = {}
    # Start with the query urls and get their job ids
    print("Archiving {} query URLS".format(len(query_urls))),

    with open(temp_dir + '/job_ids.csv', 'a') as f:
        temp = csv.writer(f)
        i = 0
        for url in set(query_urls):
            # Create a URL object
            url_obj = URLObj(url, is_seed=True)
            url_obj_dict[url] = url_obj
            url_obj.archive_url()
            # Save the url and job id to the temp file
            if url not in URLObj.job_ids:
                temp.writerow([url_obj.url, url_obj.job_id])
            i += 1
            if i % 100 == 0:
                print(f"Archived {i} URLS")
            
        # Then, get the outlinks for each of the query urls
        # We do this in stages, so that there is more time to finish the archiving,
        # instead of waiting for each one sequentially.
        i = 0
        for url_obj in url_obj_dict.values():
            i += 1
            if i % 100 == 0:
                print(f"Got outlinks for {i} URLS")
            curr_outlinks = url_obj.get_outlinks()
            if curr_outlinks is None:
                continue
            for out_url, out_job_id in curr_outlinks:
                # Use the same encoding that we'll use on the urls to archive, to make matching
                # more likely
                # More could be done here (e.g., removing parameters from URLs)
                out_url = urlencode_url(out_url)
                if out_url not in URLObj.job_ids:
                    # Save it to the class dictionary
                    URLObj.job_ids[out_url] = out_job_id
                    # And also to the temp file
                    temp.writerow([out_url, out_job_id])
        
        # Next, created instances and get the job ids for the URLs retrieved in the SERPs. Hopefully we will already have
        # some of these from the outlinks
        print("Archiving {} result URLS".format(len(urls_to_archive)))
        i = 0
        for url in set(urls_to_archive):
            i += 1
            if i % 100 == 0:
                print(f"Archived {i} URLS")
            if url in url_obj_dict:
                continue
            url_obj = URLObj(url)
            url_obj_dict[url] = url_obj
            url_obj.archive_url()
            if url not in URLObj.job_ids:
                temp.writerow([url_obj.url, url_obj.job_id])

    return url_obj_dict

def get_domain(url):
    domain = re.search('^https://www.(\w+\.\w+)', url).groups()[0]
    if not domain:
        raise ValueError("Can't find URL in {url}")
    return domain

def filter_urls(urls, domain, remove_cache):
    '''
    Separates urls into results and self-links, based on the domain.
    Skips items from the two caches:
    webcache.googleusercontent.com
    https://cc.bingj
    '''
    cache_regex = r'https://webcache.googleusercontent.com|https://cc.bingj.com'
    result = []
    self_links = []
    for url in urls:
        if remove_cache == True:
            if re.match(cache_regex, url):
                continue
        if re.match(f'https?://\w+\.?{domain}', url):
            self_links.append(url)
        else:
            result.append(url)
    return (result, self_links)

def get_urls_from_json(j_obj):
    '''Takes a JSON object and extracts the correct URLs; returns them in a list.'''
    query_url = j_obj['link']
    result = []
    
    for x in j_obj['linkElements']:
        url = x['href']
        if re.match('javascript', url) or url == '':
            continue
        result.append(url)
    return (query_url, result)

def urlencode_url(url):
    return requests.utils.requote_uri(urllib.parse.unquote_plus(url))


class URLObj:

    def __init__(self, url, is_seed = False):
        self.orig_url = url
        self.url = urlencode_url(url)
        self.job_id = self.check_for_job_id()
        self.wayback_url = self.check_for_wayback_url()
        self.is_seed = is_seed
        self.status_attempts = 0
        self.archive_attempts = 0

    def check_for_job_id(self):
        for url in [self.url, self.orig_url]:
            if url in self.job_ids: # Check in class variable list of job ids
                return self.job_ids[url]


    def check_for_wayback_url(self):
        for url in [self.url, self.orig_url]:
            if url in self.wayback_urls: # Check in class variable list of job ids
                return self.wayback_urls[url]

    def _call_status_url(self,
                         wait = 2, # Initial wait time
                         max_wait = 7 # Stop when wait time between calls hits max_wait
                        ):
        '''Helper function to handle the call to the status API'''
        job_id = self.job_id
        if job_id is None:
            return None
        s = requests.get(ENDPT + 'status/' + job_id, headers=HEADERS)
        if s.status_code == 200:
            s_json = s.json()
            if s_json['status'] == 'pending':
                if wait > max_wait:
                    logging.debug(s_json)
                    if self.status_attempts == 2:
                        logging.warning(f"The call to get the status of '{self.url}' with job id {self.job_id} failed three times. Skipping")
                        return None
                    self.status_attempts += 1
                    self.job_id = None # Get new job id and try again
                    self.archive_url()
                    return self._call_status_url()
                logging.info(f'Pending, now waiting for {wait:.2f} seconds')
                time.sleep(wait)
                return self._call_status_url(wait = wait * 1.2)
            if s_json['status'] == 'success':
                return s_json
            if s_json['status'] == 'error':
                logging.error('Could not get status, with error: {}'.format(s_json["message"]))
                return None
            else:
                logging.warning(s_json)
                raise ValueError("Status was unexpected")
        ## TODO: This error handling is horrible and is duplicated across the two calls.
        ## I know there is a much better way to do this but I don't know what it is :)
        if s.status_code == 429:
            logging.info(f'Hit rate limit, now waiting for {wait} seconds')
            time.sleep(wait)
            return self._call_status_url(wait = wait * 1.2) # Backoff
        if s.status_code in [104,502,503,504,443,401]:
            if s.status_code == 443 or s.status_code ==401:
                self.status_attempts += 1
            logging.warning('443, 502, 503, or 504 status received; waiting 30 seconds')
            logging.warning(s.text)
            time.sleep(30)
            return self._call_status_url(wait)
        else:
            s.raise_for_status()

    def get_outlinks(self):
        logging.info(f"Getting outlinks for {self.url} with job id {self.job_id}")
        job_id = self.job_id
        s_json = self._call_status_url()
        if s_json is None:
            return []
        if 'original_job_id' in s_json:
            self.job_id = s_json['original_job_id']
            return self.get_outlinks()

        try:
            return s_json['outlinks'].items()
        except KeyError:
            logging.warning(f"No outlinks for {self.url} but they were expected")
            return []
        except AttributeError:
            logging.info(f"Earlier job ({self.job_id}) didn't request outlinks for {self.url}")
            return []


    def get_wayback_url(self):
        if not self.wayback_url:
            self._retrieve_wayback_url()
        return self.wayback_url


    def _retrieve_wayback_url(self):
        logging.info(f"Getting wayback URL for {self.url} with job id {self.job_id}")
        if not self.job_id:
            self.archive_url()
        job_id = self.job_id
        s_json = self._call_status_url()
        if s_json is None:
            self.wayback_url = ''
            return None
        try:
            self.wayback_url = 'http://web.archive.org/web/{}/{}'.format(s_json['timestamp'],
                                                             s_json['original_url'])
        except KeyError:
            logging.error(f"Missing timestamp or original URL for {job_id}")
            self.wayback_url = None
    
    def archive_url(self,
                    wait = 2,
                    capture_screenshot = 0 # Whether to capture a screenshot (default is no)
                    ):
        '''Archive the url in self.url and store the job_id in self.job_id'''


        # If it already exists, then there's nothing to do
        if self.job_id is not None:
            logging.info(f'Job id already exists for {self.orig_url}')
            return None

        logging.info(f"Archiving {self.orig_url}")
        # If this is a query URL / seed URL, then capture outlinks
        capture_outlinks =  1 if self.is_seed else 0
        if self.status_attempts == 1: # If we've already tried 2 times, then try w/o outlinks
            capture_outlinks = 0


        payload = {'url': self.url,
                  'if_not_archived_within' : IF_NOT_ARCHIVED_WITHIN,
                  'capture_screenshot': capture_screenshot,
                  'capture_outlinks': capture_outlinks
                  }
        r = requests.post(ENDPT, headers=HEADERS, data=payload)
        logging.debug(r.content)

        if r.status_code == 429:
            logging.info(f'Hit rate limit, now waiting for {wait:.2f} seconds')
            time.sleep(wait)
            return self.archive_url(wait = wait * 1.2)
        if r.status_code in [104,502,503,504,443,401]:
            if s.status_code in [104, 401, 443]:
                self.archive_attempts += 1
            if self.archive_attempts > 3:
                return None
            logging.warning(self.url)
            logging.warning(r.text)
            logging.warning('502 or 503 or 504 status received; waiting 30 seconds')
            time.sleep(30)
            return self.archive_url()
        r.raise_for_status()
        self.job_id = r.json()['job_id']




if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()

