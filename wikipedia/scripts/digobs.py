#!/usr/bin/env python3

import sys
import subprocess
import logging

def git_hash(short=False):
    if short:
        return subprocess.check_output(['git', 'rev-parse', '--short', 'HEAD']).decode().strip()
    else:
        subprocess.check_output(['git', 'rev-parse', 'HEAD']).decode().strip()

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


