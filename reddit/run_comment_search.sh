#!/bin/bash
module load parallel_sql
source ./bin/activate
psu --del --Y
cat comment_search_tasks.sh | psu --load

for job in $(seq 1 50); do sbatch checkpoint_parallelsql.sbatch; done;
