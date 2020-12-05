#!/usr/bin/python
import argparse
import ast
import json
import logging
import parser
import pandas as pd

from doltpy.core import system_helpers, Dolt

# Custom Log Levels
from doltpy.core.system_helpers import get_logger
from doltpy.etl import get_df_table_writer

VERBOSE = logging.DEBUG - 1
logging.addLevelName(VERBOSE, "VERBOSE")

# Dolt Logger
logger = get_logger(__name__)

# Argument Parser Setup
parser = argparse.ArgumentParser(description='Arguments For Presidential Tweet Archiver')
parser.add_argument("-log", "--log", help="Set Log Level (Defaults to WARNING)",
                    dest='logLevel',
                    default='WARNING',
                    type=str.upper,
                    choices=['VERBOSE', 'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'])


def main(arguments: argparse.Namespace):
    # Set Logging Level
    logging.Logger.setLevel(system_helpers.logger, arguments.logLevel)  # DoltPy's Log Level
    logger.setLevel(arguments.logLevel)  # This Script's Log Level

    repoPath = 'presidential-tweets'  # tests
    table = 'trump'

    # Defaults
    branch = 'master'

    repo: Dolt = Dolt(repoPath)
    fix_json(repo=repo, table=table, branch=branch)


def fix_json(repo: Dolt, table: str, branch: str):
    get_json_query = '''
        select * from {table} order by id asc
    '''.format(table=table)

    results = repo.sql(get_json_query, result_format='json')["rows"]

    for result in results:
        fixed_result = result

        try:
            fixed_result["json"] = json.dumps(ast.literal_eval(fixed_result["json"]))
        except ValueError:
            continue

        logger.warning("Fixing Tweet {tweet_id}".format(tweet_id=fixed_result["id"]))

        df = getDataFrame(row=result)
        writeData(repo=repo, table=table, dataFrame=df, requiredKeys=['id'])


def getDataFrame(row: dict) -> pd.DataFrame:
    # Import JSON Into Panda DataFrame
    return pd.DataFrame([row])


def writeData(repo: Dolt, table: str, dataFrame: pd.DataFrame, requiredKeys: list):
    # Prepare Data Writer
    raw_data_writer = get_df_table_writer(table, lambda: dataFrame, requiredKeys)

    # Write Data To Repo
    raw_data_writer(repo)


if __name__ == '__main__':
    # This is to get DoltPy's Logger To Shut Up When Running `this_script.py -h`
    logging.Logger.setLevel(system_helpers.logger, logging.CRITICAL)

    args = parser.parse_args()
    main(args)
