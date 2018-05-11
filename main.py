#!/usr/bin/python

import argparse
from pyspark_json2parquet import workflow

'''
    this job fetches json files from the source and convert them to 
    parquet with snappy compression and store them at the pre-defined destination
    . it is re runnable, there will be no duplicate data sets created. 
    . if no date is provided to the hour, then the current hour is taken
    . destination is geared towards hive ddl partition optimizations
    . source path should start in a forward slash or s3://
    . source path should end in a forward slash
'''

def check_source(source_path):
    if source_path[-1:] != '/':
        raise argparse.ArgumentError('--source arg does not end in a /')

    if not ( source_path[:5] == 's3://' or source_path[0] == '/' ):
        raise argparse.ArgumentError('--source arg does not start in a / or s3://')

    return source_path

def check_env(env):

    if env not in {'prod', 'qa', 'dev'}:
        raise argparse.ArgumentError('--env not one of ( prod, qa, dev )')

    return env

if __name__ == '__main__':

    parser = argparse.ArgumentParser()

    parser.add_argument("--source", required=True,
                        help="source directory prefix should end in / or start in / or s3://",
                        type=check_source)

    parser.add_argument("--env", required=True,
                        help="env should in one of prod, qa, dev",
                        type=check_env)

    parser.add_argument("--date", required=False, help="optional --date yyyy-mm-dd hh24")

    argument = parser.parse_args()

    if argument.date:
        workflow.run_job(argument.source, argument.env, argument.date)
    else:
        workflow.run_job(argument.source, argument.env, None)

