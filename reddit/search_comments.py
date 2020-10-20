import pyarrow as pa
import pyarrow.dataset as ds
from datetime import datetime, timedelta

PARQUET_PATH = '/gscratch/comdata/output/reddit_comments_by_subreddit.parquet'

from_date = datetime(2019,10,1)

pq_dataset = ds.dataset(PARQUET_PATH,format='parquet')
