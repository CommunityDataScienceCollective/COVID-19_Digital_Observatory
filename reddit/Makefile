SHELL:=/bin/bash
comment_search_tasks.sh:gen_search_tasks.py
	python3 gen_search_tasks.py /gscratch/comdata/output/reddit_comments_by_subreddit.parquet comment_search_tasks.sh

submit_backfill_jobs:comment_search_tasks.sh
	./run_comment_search.sh

PHONY:submit_backfill_jobs
