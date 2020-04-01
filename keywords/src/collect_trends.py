# this follows a similar approach to nick's trends.js but in python
from pytrends.request import TrendReq
from datetime import datetime
from os import path
import csv
from itertools import islice, chain, zip_longest
import pandas as pd


# from itertools recipes
#https://docs.python.org/3.6/library/itertools.html#itertools-recipes
def grouper(iterable, n, fillvalue=None):
    "Collect data into fixed-length chunks or blocks"
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx"
    args = [iter(iterable)] * n
    return zip_longest(*args, fillvalue=fillvalue)

def get_daily_trends():
    trendReq = TrendReq(backoff_factor=0.2)
    today_trending = trendReq.today_searches()
    daily_trends_outfile = path.join("..","output","daily_google_trends.csv")

    write_header = False
    header = ['date','term','top']

    if not path.exists(daily_trends_outfile):
        write_header = True

    with open("../output/intermediate/daily_google_trends.csv",'a',newline='') as of:
        writer = csv.writer(of)
        if write_header:
            writer.writerow(header)

        for i, trend in enumerate(today_trending):
            writer.writerow([str(datetime.now().date()),trend,i])

def get_related_queries(stems):
    # we have to batch these in sets of 5
    trendReq = TrendReq(backoff_factor=0.2)
    def _get_related_queries(chunk):
        kw_list = list(filter(lambda x: x is not None, chunk))
        trendReq.build_payload(kw_list=kw_list)
        related_queries = trendReq.related_queries()
        for term, results in related_queries.items():
            for key, df in results.items():
                if df is not None:
                    df["term"] = term
                yield (key,df)

    l = chain(*map(_get_related_queries, grouper(stems,5)))
    out = {}
    for key, value in l:
        if key in out:
            out[key].append(value)
        else:
            out[key] = [value]

    for k in out.keys():
        df = pd.concat(out[k])
        df['date'] = str(datetime.now().date())
        out[k] = df
        outfile = path.join('..','output','intermediate',f"related_searches_{k}.csv")
        if path.exists(outfile):
            mode = 'a'
            header = False
        else:
            mode = 'w'
            header = True

        df.to_csv(outfile, mode=mode, header=header,index=False)

stems = [t.strip() for t in open("../resources/base_terms.txt",'r')]

get_daily_trends()

get_related_queries(stems)
