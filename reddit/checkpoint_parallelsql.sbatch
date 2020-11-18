#!/bin/bash
## parallel_sql_job.sh
#SBATCH --job-name=find_covid_comments
## Allocation Definition
#SBATCH --account=comdata-ckpt
#SBATCH --partition=ckpt
## Resources
## Nodes. This should always be 1 for parallel-sql.
#SBATCH --nodes=1    
## Walltime (12 hours)
#SBATCH --time=12:00:00
## Memory per node
#SBATCH --mem=32G
#SBATCH --cpus-per-task=4
#SBATCH --ntasks=1
#SBATCH -D /gscratch/comdata/users/nathante/COVID-19_Digital_Observatory/reddit
source ./bin/activate
module load parallel_sql
echo $(which perl)
#Put here commands to load other modules (e.g. matlab etc.)
#Below command means that parallel_sql will get tasks from the database
#and run them on the node (in parallel). So a 16 core node will have
#16 tasks running at one time.
parallel-sql --sql -a parallel --exit-on-term --jobs 4
