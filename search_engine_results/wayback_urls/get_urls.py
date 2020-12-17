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
from collections import Counter

SKIPPED_COUNT = 3 # Even when we filter self-links (e.g., links to bing from a bing query), we may want to keep those which
# appear across multiple queries. This count says how often a self-link URL has to appear for us to archive it.

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
        Adds them to the skipped_urls list, which we process at the end
        4. Filters out URLs that appear more than once in this list
        '''
        if ignore_self_links:
            domain = get_domain(query_url)
        else:
            domain = None
        cache_regex = r'https?://webcache.googleusercontent.com|https?://cc.bingj.com'

        result = set()
        for url in urls:
            if url in to_archive:
                continue

            if remove_cache == True:
                if re.match(cache_regex, url):
                    continue

            if ignore_self_links and re.match(f'https?://\w*\.?{domain}', url):
                skipped_urls.append(url)
            else:
                result.add(url)
        return result

    skipped_urls = []
    to_archive = {}
    for fn in files:
        q_url, link_urls = get_urls(fn)
        to_archive[q_url] = 'query'
        for url in link_urls:
            # Prioritize query urls - if it's already there,
            # then don't overwrite
            if url not in to_archive:
                to_archive[url] = 'link'

    skipped_to_keep = filter_skipped_urls(skipped_urls, SKIPPED_COUNT)
    for url in skipped_to_keep:
        if url not in to_archive:
            to_archive[url] = 'link'

    write_urls(to_archive, output_file)


def filter_skipped_urls(urls, n): # n is the number of times a URL must appear in order to be archived
    result = set()
    for url, count in Counter(urls).items():
        if count >= n:
            result.add(url)
    return result


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
