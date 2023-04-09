"""
This is a notebook that tests the main script 2_batch_report_sync line by line with outputs for the purpose of debugging. 
Expected to be run on Stanford's production environment.

Instruction:
on Sherlock command line, run: 
singularity exec $GROUP_HOME/singularity/epa_7-08-21_2.sif python3 -m 2_batch_report_sync --mode <e.g. test> --run_id <e.g. 2021Q3_2021-07-08_100009_688526>

The main script follows these steps:
1. sync s3 bucket with stanford's sherlock oak folder
2. upload batch reports to reglab's aws database
3. generate ky batch report as pdfs of emails

Prerequistes: 
1. must have run 0_email_maker and 1_whippet_sender first 
2. must have batch reports generated in s3 bucket
3. must have programmatic permisssions to the s3 bucket
4. must have aws and credentials set up on sherlock. default credentials are set to Nicole's credentials (hongjinl@law.stanford.edu)

See line-by-line test in tests/line_by_line_test_batch_report_sync.ipynb: https://github.com/reglab/esnc_risk_notif/blob/main/code/tests/line_by_line_test_batch_report_sync.ipynb

This script will take about 1 min to run. 
"""

# import installed packages
import os
import argparse
from io import StringIO

import pandas as pd
import datetime as dt
import logging

# for generating pdf
import pdfkit 

## for s3 connection
import boto3
import subprocess

# import supporting modules
import configs
from utilities import sql_save

def get_args():
    """
    Use argparse to parse command line arguments.
    """
    parser = argparse.ArgumentParser(description="Use argparse to parse command line arguments. Required: mode, run_id.")

    parser.add_argument(
        '--mode',
        help='The runner mode for whippet_sender. Available options: prod and test. prod refers production mode and test refers to testing mode.',
        type=str,
        required=True,
        default='test'
    )
    parser.add_argument(
        '--run_id',
        help="The id of the email_maker run, required for reading email files from S3 bucket, in <fiscal quarter>_<runtime YYYY-MM-DD_HHMMSS> format, e.g. 2021Q3_2021-07-02_104040_958240",
        type=str,
        required=True
    )

    args = parser.parse_args()

    return args

def main():
    """
    Sync batch report to Stanford's Sherlock Oak folder and upload it to RegLab's AWS databsae. 
    """
    print(configs.HELPER_TEXT_BATCH_REPORT_SYNC)
    print("===== Start running batch report sync =====")

    ## get parsed variables
    args = get_args()
    mode = args.mode
    run_id = args.run_id

    ## get global variables
    engine = configs.GET_ENGINE()
    bucket = configs.BUCKET
    s3_project_dir = configs.S3_PROJECT_DIR
    oak_project_dir = configs.GET_OAK_PROJECT_DIR()

    ## set directories
    s3_run_dir = os.path.join(mode, run_id)
    oak_run_dir = os.path.join(oak_project_dir, mode, run_id)
    oak_log_dir = os.path.join(oak_run_dir, 'logs')

    ## configure logging
    logger, log_capture_string = configs.configure_logging(logger_name = 'batch_report_sync')
    logger.info(configs.HELPER_TEXT_BATCH_REPORT_SYNC)
    logger.info("Configured logger")
    logger.info(f"Log file to be saved in {oak_log_dir}")
    logger.info("----- Parsed variables: mode = {}, run_id = {}".format(mode, run_id))
    logger.info("----- S3 bucket: s3_project_dir = {}, s3_run_dir = {}".format(s3_project_dir, s3_run_dir))
    logger.info("----- Sherlock OAK folders: oak_run_dir = {}".format(oak_run_dir))

    ## print out variables and let the user confirm if they are correct and wish to proceed. 
    print("----- Parsed variables: mode = {}, run_id = {}".format(mode, run_id))
    print("----- S3 bucket: s3_project_dir = {}, s3_run_dir = {}".format(s3_project_dir, s3_run_dir))
    print("----- Sherlock OAK folders: oak_run_dir = {}".format(oak_run_dir))

    proceed = input('Please verify the above variables. Do you wish to proceed with the run? [y/n]')

    # if proceed, run the rest of the script
    if proceed == 'y':
        logger.info("========= 1/4 Sync s3 bucket with Stanford's Sherlock OAK folder ==========")
        subprocess.run(['aws', 's3', 'sync', s3_project_dir, oak_project_dir])

        logger.info("======== 2/4 read batch report and upload it to reglab's aws database ========")
        batch_report = pd.read_csv(os.path.join(oak_run_dir, 'batch_report.csv'))
        batch_report['run_id'] = run_id
        batch_report['file_timestamp'] = dt.datetime.now()
        sql_save.save_batch_report(mode = mode, batch_report = batch_report, engine = engine)

        logger.info("======== 3/4 generate pdfs of emails for KY ==========")
        batch_report_ky = pd.read_csv(os.path.join(oak_run_dir, 'batch_report_ky.csv'))
        batch_report_ky = batch_report_ky.sort_values(by = 'npdes_permit_id')
        html_str = ""
        for i in range(len(batch_report_ky)):
            email = batch_report_ky.iloc[i]
            html_str_i = f"""
                <meta http-equiv="Content-type" content="text/html; charset=UTF-8" />
                <p style="page-break-before: always;"></p>
                <p><b>Subject:</b> {email['subject']}</p>
                <p><b>From:</b> {email['email_sent_from_addr']}</p>
                <p><b>To:</b> {email['email_sent_to_addr']}</p>
                <p><b>Timestamp:</b> {email['email_sent_timestamp']} EST</p>
                <hr>
                {email['body']}
                """
            html_str = html_str + html_str_i
        pdfkit.from_string(html_str, os.path.join(oak_run_dir, 'batch_report_ky.pdf'))

        logger.info('======= 4/4 save log file and sync to s3 bucket =======')
        logger.info(f'Script FINISHED. Log file saved in {oak_log_dir} and synced with S3 bucket.')
        with open(os.path.join(oak_log_dir, 'batch_report_sync.log'), 'w') as file:
            file.write(log_capture_string.getvalue())
            
        subprocess.run(['aws', 's3', 'sync', oak_project_dir, s3_project_dir], check=True)

        print("===== Finish running batch report sync =====")
    else:
        print('Script discontinued by user. Exiting.')

if __name__ == "__main__":
    main()
