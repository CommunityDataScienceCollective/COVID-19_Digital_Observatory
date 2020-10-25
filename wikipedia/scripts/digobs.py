#!/usr/bin/env python3
from requests import Request
from datetime import datetime
import sys
import subprocess
import logging
import itertools
import requests
from functools import partial
from csv import DictWriter
import json
import mwapi as api

user_agent = "COVID-19 Digital Observatory, a Community Data Science Collective project. (https://github.com/CommunityDataScienceCollective/COVID-19_Digital_Observatory)"

def git_hash(short=False):
    if short:
        return subprocess.check_output(['git', 'rev-parse', '--short', 'HEAD']).decode().strip()
    else:
        subprocess.check_output(['git', 'rev-parse', 'HEAD']).decode().strip()


def init_logging(args):
    #handle -W
    if args.logging_destination:
        logging.basicConfig(filename=args.logging_destination, filemode='a', level=args.logging_level)
    else:
        logging.basicConfig(level=args.logging_level)

    export_git_hash = subprocess.check_output(['git', 'rev-parse', 'HEAD']).decode().strip()
    export_git_short_hash = subprocess.check_output(['git', 'rev-parse', '--short', 'HEAD']).decode().strip()
    export_time = str(datetime.now())

    logging.info(f"Starting at {export_time}.")
    logging.info(f"Last commit: {export_git_hash}")
    return(logging)

def call_view_api(page, project, query_date):
    url= f"https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/{project}/all-access/all-agents/{page}/daily/{query_date}00/{query_date}00"
    response = requests.get(url)
    if response.ok:
        return response.json().get('items',[None])[0]

    else:
        logging.warning(f"Failure: {response.status_code} from {url}")
        print(response.json().get("detail",None))
        return None

# This function writes out the view data to a json file (j_outfile)
# and a tsv file (t_outfile), keeps track of failures,
# and returns the number of successes and failures
def process_view_responses(responses, j_outfile, t_outfile, logging = None):
    
    failures = len(list(itertools.takewhile(lambda r: r is None, responses)))
    successes = 1

    try:
        first_response = next(responses)
    except StopIteration:
        logging.error("No valid responses")
        exit()

    dw = DictWriter(t_outfile, sorted(first_response.keys()), delimiter='\t')
    dw.writeheader()
    json.dump(first_response, j_outfile)
    dw.writerow(first_response)

    for response in responses:
        if response is None:
            failures = failures + 1
            continue
        else:
            successes = successes + 1

        if logging is not None:
            logging.debug(f"printing data: {response}")

        json.dump(response, j_outfile)
        dw.writerow(response)

    return (successes,failures)
        
def get_loglevel(arg_loglevel):
    loglevel_mapping = { 'debug' : logging.DEBUG,
                         'info' : logging.INFO,
                         'warning' : logging.WARNING,
                         'error' : logging.ERROR,
                         'critical' : logging.CRITICAL }

    if arg_loglevel in loglevel_mapping:
        loglevel = loglevel_mapping[arg_loglevel]
        return loglevel
    else:
        print("Choose a valid log level: debug, info, warning, error, or critical", file=sys.stderr)
        return logging.INFO


def get_revisions_for_page(title, api_session, logging, rv_props):
  
    result = api_session.get(action='query',
                             prop='revisions',
                             rvprop=rv_props.values(),
                             titles = {title},
                             rvdir='newer',
                             rvslots='*',
                             continuation=True)
    return result

def get_pages_revisions(titles, project, logging, rv_props):
    logging.info(f"pulling revisions for: {project}")

    api_session = api.Session(f"https://{project}.org/w/api.php",
                              user_agent=user_agent
                              )
    
    return itertools.chain(* map(partial(get_revisions_for_page, api_session = api_session, logging = logging, rv_props = rv_props), titles))

def rev_batch_to_json(rev, export_info, json_output = None):
    rev['exported'] = export_info
    if json_output is None:
        return json.dumps(rev)
    else:
        json.dump(rev, json_output)

def rev_batch_to_tsv(batch, tsv_fields, export_info, project, tsv_writer=None):
    batch = batch.get('query',dict())
    pages = batch.get('pages',dict())
    for pageid, page in pages.items():
        pageid = page.get('pageid',None)
        ns = page.get('ns',None)
        title = page.get('title','')
        logging.info(f"pulling revisions for: {title}")
        revs = page.get('revisions',[])
        for rev in revs:

            # handle missing data
            if "sha1" not in rev:
                rev["sha1"] = ""

            if "userhidden" in rev:
                rev["user"] = ""
                rev["userid"] = ""

            # recode anon so it's true or false instead of present/missing
            if "anon" in rev:
                rev["anon"] = True
            else:
                rev["anon"] = False

            # let's recode "minor" in the same way
            if "minor" in rev:
                rev["minor"] = True
            else:
                rev["minor"] = False

            # add page title information
            rev['title'] = title
            rev['pageid'] = pageid
            rev['namespace'] = ns
            rev['contentmodel'] = rev['slots']['main']['contentmodel']
            # construct a URL
            rev['url'] = Request('GET', f'https://{project}.org/w/index.php',
                                 params={'title' : rev['title'].replace(" ", "_"),
                                         'oldid' : rev['revid']}).prepare().url

            rev['export_timestamp'] = export_info['export_timestamp']
            rev['export_commit'] = export_info['export_commit']
            tsv_writer.writerow({k: rev[k] for k in tsv_fields})
