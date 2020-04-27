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


def main():
    parser = argparse.ArgumentParser(description='Gets URLs to archive from SERP metadata files')
    parser.add_argument('-i', help='Input directory with metadata files')
    parser.add_argument('-o', help='Location to save URL list')
    ## TODO: Maybe switch this so default is to ignore?
    parser.add_argument('--ignore_self_links', help='Whether to ignore links from the same domain as the query',
            action='store_true')

    args = parser.parse_args()

    # Make a list of the files that we are going to be editing (skip those already edited)
    files = pathlib.Path(args.i).glob('**/*.json')
    ## FOR TESTING ONLY!!!
    #files = list(files)[10:11]
    get_urls_from_files(files, args.o, args.ignore_self_links)

def get_urls_from_files(files,
        output_file,
        ignore_self_links,
        remove_cache = True):

    def get_urls(fn):
        '''Takes a file, gets the urls to archive, passes them to the archive_url function, and writes them to
        the output file'''
        with open(fn, 'r') as f:
            j_obj = json.load(f)
            # Get the URLs from the file
            query_url, link_urls = get_urls_from_json(j_obj)
            # Filter out the self links and search engine cache urls
            link_urls = filter_link_urls(query_url, link_urls)
            return (query_url, link_urls)

    def filter_link_urls(query_url,
                         urls):
        '''
        Takes link urls and filters them in four ways:
        1. (Optionally) Ignores urls from the two caches:
        webcache.googleusercontent.com
        https://cc.bingj
        2. Filters out those which are in the completed_urls dictionary
        3. (Optionally) Identifies URLs which have the same domain as the query URL.
        Checks the skipped_urls list to see if the URL already appears there. If so, we assume
        that we want it archived and move it from skipped to the to_archive list
        4. Filters out URLs that appear more than once in this list
        '''
        if ignore_self_links:
            domain = get_domain(query_url)
        else:
            domain = None
        cache_regex = r'https://webcache.googleusercontent.com|https://cc.bingj.com'

        result = set()
        for url in urls:
            if url in to_archive:
                continue

            if remove_cache == True:
                if re.match(cache_regex, url):
                    continue

            if ignore_self_links and re.match(f'https?://\w*\.?{domain}', url):
                # If it matches, check if it's in skipped URLs
                # If so, remove it from there, and add it to the to_archive list
                if url in skipped_urls:
                    result.add(url)
                    skipped_urls.remove(url)
                # Else, add it to the skipped urls (and skip it)
                else:
                    skipped_urls.add(url)
            else:
                result.add(url)
        return result

    skipped_urls = set()
    to_archive = {}
    for fn in files:
        q_url, link_urls = get_urls(fn)
        to_archive[q_url] = 'query'
        for url in link_urls:
            # Prioritize query urls - if it's already there,
            # then don't overwrite
            if url not in to_archive:
                to_archive[url] = 'link'
    write_urls(to_archive, output_file)


def write_urls(url_dict, fn):
    with open(fn, 'w') as out_file:
        f = csv.writer(out_file)
        for url, link_type in url_dict.items():
            f.writerow([url, link_type])


def get_domain(url):
    domain = re.search('^https://www.(\w+\.\w+)', url).groups()[0]
    if not domain:
        raise ValueError("Can't find URL in {url}")
    return domain


def get_urls_from_json(j_obj):
    '''Takes a JSON object and extracts the correct URLs; returns them in a list.'''
    query_url = urlencode_url(j_obj['link'])
    link_urls = set()

    for x in j_obj['linkElements']:
        url = x['href']
        if re.match('javascript', url) or url == '':
            continue
        link_urls.add(urlencode_url(url))
    return (query_url, link_urls)


def urlencode_url(url):
    return requests.utils.requote_uri(urllib.parse.unquote_plus(url))


if __name__ == '__main__':
    main()
