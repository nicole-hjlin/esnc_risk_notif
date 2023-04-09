"""
Main script to read and send emails from JSON files and upload batch report to s3 for the ESNC Risk Notification Project. 
Expected to be run on EPA's production environment Whippet. 

Instruction:
test: on Sherlock command line, run: 
singularity exec $GROUP_HOME/singularity/epa_7-08-21_2.sif python3 -m 1_whippet_sender --mode test --run_id <e.g. 2021Q3_2021-07-08_100009_688526> --system <e.g. sherlock>
prod: on whippet command line, run (not tested)
python3 -m 1_whippet_sender --mode prod --run_id <e.g. 2021Q4_2021-08-04_140828_785316 for 2021Q4 launch> --system whippet

The main script follows these steps:
1. import installed packages and supporting modules
2. set up directories and logging
3. read email files from s3 bucket
4. send email from whippet
5. generate batch report 
6. save batch report to s3 bucket
7. save logging file and sync with sherlock folde

Prerequistes: 
1. must have run 0_email_maker first 
2. must have the email JSON files generated in s3 bucket
3. must have programmatic permisssions to the s3 bucket
4. must have aws and credentials set up on whippet. default credentials are set to Nicole's credentials (hongjinl@law.stanford.edu)

See line-by-line test in tests/line_by_line_test_whippet_sender.ipynb: https://github.com/reglab/esnc_risk_notif/blob/main/code/tests/line_by_line_test_whippet_sender.ipynb

This script would take about 1 min to run. Check emails sent to reglabtest@gmail.com (test mode) and hongjinl@law.stanford.edu (test and prod mode). 
"""

# import installed packages
import os
import argparse

import pandas as pd
import datetime as dt
import logging

## for s3 connection
import boto3
import subprocess

## for emailer
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib, ssl

# import supporting modules
import configs
from utilities import json_functions

def get_args():
    """
    Use argparse to parse command line arguments.

    Arguments
    --------
    mode: string
        The runner mode for whippet_sender. Available options: prod and test. prod refers production mode and test refers to testing mode.
    run_id: string
        The id of the email_maker run, required for reading email files from S3 bucket, in <fiscal quarter>_<runtime YYYY-MM-DD_HHMMSS> format, e.g. 2021Q3_2021-07-02_104040_958240
    system: string
        The system from where we are sending out the emails. Options: sherlock (Stanford) or whippet (EPA).
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
    parser.add_argument(
        '--system',
        help="The system from where we are sending out the emails. Options: sherlock (Stanford) or whippet (EPA).",
        type=str,
        required=True
    )

    args = parser.parse_args()

    return args

def main():
    """
    Send emails from whippet and generate batch report. 
    """
    print(configs.HELPER_TEXT_WHIPPET_SENDER)
    print("===== Start running whippet sender =====")

    ## get parsed variables
    args = get_args()
    mode = args.mode
    run_id = args.run_id
    system = args.system
    assert mode in ['test', 'prod'], 'Expect mode to be in test or prod. Aborting.'
    assert system in ['sherlock', 'whippet'], 'Expect system to be in sherlock or whippet. Aborting.'

    ## get global variables
    bucket = configs.BUCKET
    s3_project_dir = configs.S3_PROJECT_DIR
    prod_from_addr = configs.PROD_FROM_ADDR
    test_from_addr = 'reglabtest@gmail.com' # 2021-08-04 hardcoded temporarily. Ideally, we would want to add an environment variable to the whippet system. configs.TEST_FROM_ADDR
    test_to_addr = 'reglabtest@gmail.com' # see above. configs.TEST_TO_ADDR
    test_addr_pwd = configs.TEST_ADDR_PWD
    test_bcc_addr = configs.TEST_BCC_ADDR  

    ## set directories based on mode and run_id 
    s3_run_dir = os.path.join(mode, run_id)
    s3_emails_dir = os.path.join(s3_run_dir, 'emails')
    s3_log_dir = os.path.join(s3_run_dir, 'logs')

    ## configure logging
    logger, log_capture_string = configs.configure_logging(logger_name = 'whippet_sender')
    logger.info(configs.HELPER_TEXT_WHIPPET_SENDER)
    logger.info("Configured logger")
    logger.info("----- Parsed variables: mode = {}, run_id = {}".format(mode, run_id))
    logger.info("----- S3 bucket: s3_project_dir = {}, s3_run_dir = {}".format(s3_project_dir, s3_run_dir))
    logger.info("----- From email address: {}".format(test_from_addr if system == 'sherlock' else prod_from_addr))
    logger.info("----- To email address: {}".format(test_to_addr if mode == 'test' else 'facility addresses'))

    ## print out variables and let the user confirm if they are correct and wish to proceed. 
    print("----- Parsed variables: mode = {}, run_id = {}".format(mode, run_id))
    print("----- S3 bucket: s3_project_dir = {}, s3_run_dir = {}".format(s3_project_dir, s3_run_dir))
    print("----- From email address: {}".format(test_from_addr if system == 'sherlock' else prod_from_addr))
    print("----- To email address: {}".format(test_to_addr if mode == 'test' else 'facility addresses'))

    proceed = input('Please verify the above variables. Do you wish to proceed with the run? [y/n]')

    # if proceed, run the rest of the script
    if proceed == 'y':
        logger.info('====== 1/7 Reading email files from s3 bucket =======')
        email_dicts = json_functions.read_emails_from_json(s3_emails_dir, s3=True, bucket=bucket)

        logger.info('====== 2/7 Generating email objects with dictionaries =======')
        notif_permits = [e['npdes_permit_id'] for e in email_dicts]
        logger.info(f'for the following permit ids (totalling {len(notif_permits)} permits): {notif_permits}')
        
        logger.info(f'======== 3/7 Sending emails with whippet sender mode: {mode} =========') 
        # compile emails
        for email in email_dicts:
            logger.info(f"Compiling email for {email['npdes_permit_id']}")
            msg = MIMEMultipart('alternative')
            msg['Subject'] = email['subject']
            msg.attach(MIMEText(email['header'] + email['body'], 'html'))
            email['whippet_sender_mode'] = mode

            # send emails
            logger.info(f'Sending email to test email address {test_to_addr}')
            if mode == 'test':
                msg['To'] = test_to_addr
                msg['BCC'] = test_bcc_addr

                # sending out from sherlock: using a test gmail account
                if system == 'sherlock': 
                    msg['From'] = test_from_addr
                    password = test_addr_pwd
                    port = 465  # For SSL
                    smtp_server = "smtp.gmail.com"
                    context = ssl.create_default_context()
                    with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
                        server.login(msg['From'], password)
                        server.send_message(msg)

                # sending out from whippet: using epa's production email 
                ## to be tested on whippet
                if system == 'whippet':
                    msg['From'] = prod_from_addr
                    with smtplib.SMTP('localhost', port=25) as server: 
                        server.send_message(msg)
            
            elif mode == 'prod':
                msg['From'] = prod_from_addr
                msg['To'] = email['to_addrs']
                msg['BCC'] = email['bcc_addrs']

                with smtplib.SMTP('localhost', port=25) as server: 
                    server.send_message(msg)
            
            email['email_sent_timestamp'] = dt.datetime.now()
            email['email_sent_from_addr'] = msg['From']
            email['email_sent_to_addr'] = msg['To']
            email['bcc_addrs'] = msg['BCC']

        logger.info('====== 4/7 Generating batch report as dataframe =======')
        cols = ['npdes_permit_id', 
                'fiscal_quarter', 
                'to_addrs', 
                'bcc_addrs',
                'whippet_sender_mode',
                'email_sent_timestamp',
                'email_sent_from_addr',
                'email_sent_to_addr',
                'email_template',
                'subject',
                'header',
                'body'
               ]
        batch_report = pd.DataFrame(email_dicts)[cols]

        logger.info('======= 5/7 Saving batch report to s3 bucket ========')
        ## to_csv works on sherlock but not on whippet. Missing dependency within pandas
        ## work around: saving csv to the repo folder first, then copy that to S3 bucket, then remove the csv from the repo folder.
        #batch_report.to_csv(os.path.join(s3_project_dir, s3_run_dir,'batch_report.csv'), index = False)
        batch_report.to_csv('batch_report.csv', index = False)

        logger.info('======= 6/7 Subset and save KY batch report to s3 bucket ======')
        ky_batch_report = batch_report[batch_report.npdes_permit_id.str.startswith('KY')]
        #ky_batch_report.to_csv(os.path.join(s3_project_dir, s3_run_dir, 'batch_report_ky.csv'))
        ky_batch_report.to_csv('batch_report_ky.csv', index=False)

        os.system(f'aws s3 cp batch_report.csv {s3_project_dir}/{s3_run_dir}/batch_report.csv')
        os.system(f'aws s3 cp batch_report_ky.csv {s3_project_dir}/{s3_run_dir}/batch_report_ky.csv')
        os.system('rm batch_report*.csv')

        logger.info('========= 7/7 Saving logging file to s3 bucket =========')
        logger.info(f'Script FINISHED. Log file saved in S3 bucket {s3_log_dir}. Note: not yet synced with Sherlock oak folder.')
        logger_obj = bucket.Object(os.path.join(s3_log_dir, 'whippet_sender.log'))
        logger_obj.put(Body=log_capture_string.getvalue())

        print("===== Finish running whippet sender =====")
    else:
        print('Script discontinued by user. Exiting.')

if __name__ == "__main__":
    main()
    
