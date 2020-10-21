import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pandas as pd
import fire
import ahocorasick
from datetime import datetime, timedelta
from pathlib import Path

PARQUET_PATH = Path('/gscratch/comdata/output/reddit_comments_by_subreddit.parquet')
OUTPUT_PATH = Path('/gscratch/comdata/users/nathante/COVID-19_Digital_Observatory/reddit/filtered_comments.parquet')
def load_keywords(keywords):
    keywords = pd.read_csv(keywords)

    keywords = set(keywords.label)
    keywords = map(str.lower, keywords)
    trie = ahocorasick.Automaton()
    list(map(lambda s: trie.add_word(s,s),keywords))
    trie.make_automaton()
    return(trie)

# use the aho corasick algorithm to do the string matching
def match_comment_kwlist(body,trie,min_length=5):
    stems = trie.iter(body.lower())
    stems = map(lambda s: s[1], stems)
    stems = filter(lambda s: len(s) >= 5, stems)
    return list(stems)

def filter_comments(partition, keywords="../keywords/output/csv/2020-10-19_wikidata_item_labels.csv", from_date = datetime(2019,10,1)):

    if partition is None:
        partition_path = next(PARQUET_PATH.iterdir())
        partition = partition_path.stem
    else:
        partition_path = PARQUET_PATH / partition

    trie = load_keywords(keywords)

    pq_dataset = ds.dataset(partition_path)

    batches = pq_dataset.to_batches(filter=(ds.field("CreatedAt")>=from_date))

    for batch in batches:
        df = batch.to_pandas()
        if df.shape[0] > 0:
            matches = df.body.apply(lambda b: match_comment_kwlist(b, trie, min_length=5))
            has_match = matches.apply(lambda l: len(l) > 0)
            df = df.loc[has_match]
            df['keyword_matches'] = matches[has_match]
            if df.shape[0] > 0:
                df.to_parquet(OUTPUT_PATH / f'{partition}',index=False,engine='pyarrow',flavor='spark')
     

if __name__ == "__main__":
    fire.Fire(filter_comments)
