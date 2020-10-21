import fire
from pathlib import Path


def gen_tasks("/gscratch/comdata/output/reddit_comments_by_subreddit.parquet", outfile):
    path = Path(in_parquet)
    partitions = path.glob("*.parquet")
    partitions = map(lambda p: p.stem, partitions)
    base_task = "python3 search_comments.py {0}"

    lines = map(base_task.format, partitions)

    with open(outfile,'w') as of:
        of.writelines(map(lambda l: l + '\n',lines))

if __name__ == "__main__":
    fire.Fire(gen_tasks)
