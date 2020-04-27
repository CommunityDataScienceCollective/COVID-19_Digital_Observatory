#!/usr/bin/env python
# coding: utf-8

import pathlib
import json
import re
import time
import argparse
import logging
import csv
import requests
import urllib



def main():
    parser = argparse.ArgumentParser(description='Add wayback URLs to SERP metadata.')
    parser.add_argument('-i', help='Input directory with metadata files')
    parser.add_argument('-w', help = 'Location of file with wayback URLS')
    parser.add_argument('-o', help='Directory to save modified files (if blank or same as input directory, will overwrite)')

    args = parser.parse_args()

    
    # Make a list of the files that we are going to be editing (skip those already edited)
    files = pathlib.Path(args.i).glob('**/*.json')
    wayback_dict = load_wayback_dict(args.w)
    for fn in files:
        write_wayback_to_file(fn, args.o, wayback_dict)


def write_wayback_to_file(filename, out_dir, wayback_dict):
    with open(filename, 'r') as f:
        j_obj = json.load(f)
        query_url = urlencode_url(j_obj['link'])
        try:
            wayback_url = wayback_dict[query_url]
            j_obj['wayback_url'] = wayback_url
        except KeyError:
            logging.error(f"Should have an entry for {query_url}")
            logging.error(wayback_dict.keys())
            j_obj['wayback_url'] = ''
        for link_obj in j_obj['linkElements']:
            link_url = urlencode_url(link_obj['href'])
            if link_url == '':
            try:
                wayback_url = wayback_dict[link_url]
                link_obj['wayback_url'] = wayback_url
            except KeyError:
                logging.info(f'No WB URL for {link_url}')
                link_obj['wayback_url'] = ''
    outfile = get_out_path(filename, out_dir)
    with open(outfile, 'w') as f:
        json.dump(j_obj, f)


def get_out_path(fp, out_dir):
    '''Assumes that we want to keep the directory and the file name'''
    if out_dir == None:
        return fp
    else:
        new_path = pathlib.Path(out_dir).joinpath(*fp.parts[-2:])
        if not new_path.parent.exists():
            logging.warning(f"Creating new path at {new_path}")
            new_path.parent.mkdir(parents = True)
        return new_path

def load_wayback_dict(fn):
    '''Loads the waback URL file as a dictionary of {orig_url:wb_url}. Currently ignores
    the timestamp, overwriting older WB URLs with newer ones'''
    result = {}
    if pathlib.Path(fn).exists():
        with open(fn, 'r') as f_obj:
            f = csv.reader(f_obj)
            for row in f:
                result[row[1]] = row[2]
    return result


def urlencode_url(url):
    return requests.utils.requote_uri(urllib.parse.unquote_plus(url))

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()

