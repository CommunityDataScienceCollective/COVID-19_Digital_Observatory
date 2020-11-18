import pandas as pd
import pyarrow

df = pd.read_parquet("/gscratch/comdata/users/nathante/COVID-19_Digital_Observatory/reddit/covid-19_reddit_comments.parquet")

df.to_feather("/gscratch/comdata/users/nathante/COVID-19_Digital_Observatory/reddit/covid-19_reddit_comments_18-11-20.feather")
df.to_json("/gscratch/comdata/users/nathante/COVID-19_Digital_Observatory/reddit/covid-19_reddit_comments_18-11-20.json",orient='records',lines=True)
df.to_csv("/gscratch/comdata/users/nathante/COVID-19_Digital_Observatory/reddit/covid-19_reddit_comments_18-11-20.csv",index=False)
