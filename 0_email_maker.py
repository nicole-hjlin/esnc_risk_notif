"""
Main script to generate emails for the ESNC Risk Notification Project. 
Expected to be run on Stanford RegLab's production environment (Sherlock).

The main script follows these steps:
1. import installed packages and supporting modules
2. set up directories and logging
3. grab data from aws database
4. random assignment
5. save at risk permits data and assignment
6. generate emails with at risk permits data
7. save email files and sync with s3 bucket in whippet

Instruction:
on Sherlock command line, run: 
singularity exec $GROUP_HOME/singularity/epa_7-08-21_2.sif python3 -m 0_email_maker --mode <e.g. test> --model_id <e.g. notification_intervention_2021q3_2021-04-28_144616_986680> --quarter <e.g. 2021Q3>

Prerequistes: 
1. must have access to RegLab's AWS database reglabepa and saved credentials as environment variables on sherlock
2. must have programmatic permisssions to the s3 bucket
3. must have aws and credentials set up on sherlock. default credentials are set to Nicole's credentials (hongjinl@law.stanford.edu)

See line-by-line test in tests/line_by_line_test_email_maker.ipynb: https://github.com/reglab/esnc_risk_notif/blob/main/code/tests/line_by_line_test_email_maker.ipynb

This script would take about 15 mins to run.
"""

# import installed packages
import os
import argparse
import subprocess

import random
import numpy as np
import pandas as pd
import datetime as dt

# import supporting modules
import configs
from utilities import sql_grab
from utilities import sql_save
from utilities import sql_queries
from utilities import json_functions
from utilities import templating
from utilities import random_assignment

def get_args():
    """
    Use argparse to parse command line arguments.

    Arguments
    --------
    mode: string
        The runner mode for email_maker. Available options: prod and test. prod refers production mode and test refers to testing mode.
    model_id: string
        The id of the ML model, required for reading prediction results from RegLab's AWS database, e.g. notification_intervention_2021q3_2021-04-28_144616_986680
    quarter: string
        The fiscal quarter for the model prediction results, e.g. 2021Q3
    """
    parser = argparse.ArgumentParser(description="Use argparse to parse command line arguments. Required: mode, model_id, quarter")

    parser.add_argument(
        '--mode',
        help='The runner mode for email_maker. Available options: prod and test. prod refers production mode and test refers to testing mode.',
        type=str,
        required=True,
        default='test'
    )
    parser.add_argument(
        '--model_id',
        help="The id of the ML model, required for reading prediction results from RegLab's AWS database, e.g. notification_intervention_2021q3_2021-04-28_144616_986680",
        type=str,
        required=True
    )
    parser.add_argument(
        '--quarter',
        help="The fiscal quarter for the model prediction results, e.g. 2021Q3",
        type=str,
        required=True
    )

    args = parser.parse_args()
    return args

def main(): 
    """
    main runner function
    """
    print(configs.HELPER_TEXT_EMAIL_MAKER)
    print('===== Start running email maker =====')

    ## get parsed variables
    args = get_args()
    mode = args.mode
    model_id = args.model_id
    quarter = args.quarter

    ## get global variables 
    engine = configs.GET_ENGINE()
    bucket = configs.BUCKET
    seed = configs.SEED
    states_list = configs.STATES_LIST
    classification_threshold = configs.CLASSIFICATION_THRESHOLD

    ## generate run id
    runtime = dt.datetime.now()
    runtime_str = str(runtime).replace(' ', '_').replace(':', '').replace('.', '_')
    run_id = quarter + '_' + runtime_str 

    ## create folders for the current run 
    oak_project_dir = configs.GET_OAK_PROJECT_DIR()
    s3_project_dir = configs.S3_PROJECT_DIR
    oak_run_dir = os.path.join(oak_project_dir, mode, run_id)
    assert not os.path.exists(oak_run_dir), "The output directoary already exists. Aborting."
    
    oak_log_dir = os.path.join(oak_run_dir, 'logs')
    oak_emails_dir = os.path.join(oak_run_dir, 'emails')
    list(map(os.makedirs, [oak_run_dir, oak_log_dir, oak_emails_dir]))

    ## configure logging
    logger, log_capture_string = configs.configure_logging(logger_name = 'email_maker')
    logger.info(configs.HELPER_TEXT_EMAIL_MAKER)
    logger.info("Configured logger")
    logger.info("----- Parsed variables: mode = {}, model_id = {}, quarter = {}".format(mode, model_id, quarter))
    logger.info("----- Project variables: states_list = {}, classification_threshold = {}".format(states_list, classification_threshold))
    logger.info("----- Sherlock OAK folders: oak_run_dir = {}".format(oak_run_dir))
    logger.info("----- S3 bucket: s3_project_dir = {}".format(s3_project_dir))

    ## print out variables and let the user confirm if they are correct and whether to proceed. 
    print("----- Parsed variables: mode = {}, model_id = {}, quarter = {}".format(mode, model_id, quarter))
    print("----- Project variables: states_list = {}, classification_threshold = {}".format(states_list, classification_threshold))
    print("----- Sherlock OAK folders: oak_run_dir = {}".format(oak_run_dir))
    print("----- S3 bucket: s3_project_dir = {}".format(s3_project_dir))

    proceed = input('Please verify the above variables. Do you wish to proceed with the run? [y/n]')

    # if proceed, run the rest of the script
    if proceed == 'y':
        logger.info("===== 1/6 Get model result table and database update date =====")

        data = sql_grab.get_at_risk_permits(model_id = model_id, states_list = states_list, classification_threshold = classification_threshold, engine = engine)
        ## subset to permits with historical violation records
        missing_violation_count = sum(data.known_violations.isna())
        logger.info(f"Excluding {missing_violation_count} permits without any historical violation records from DMRs in the past three years.")
        data = data[~data.known_violations.isna()]
        assert len(data) != 0, "Expect more than one at-risk permits. Aborting."
        logger.info(f"Number of at-risk permits = {len(data)}")

        database_update_date = sql_grab.get_database_update_date(engine = engine)
        logger.info(f"RegLab AWS database update date: {database_update_date}")

        logger.info("====== 2/6 Random Assignment ======")
        logger.info("--- Level 1: randomly split into 50-50. We will send notifications to half of the permits.")
        data = random_assignment.level_one_randomization(df=data, treatment_fraction=0.5, random_seed=seed)
        assert 'notification_flag' in data.columns.tolist(), "Expect a column named notification_flag after level one randomization. Aborting."
        assert sum(data.notification_flag.isna()) == 0, "Expect every permit to have an assignment. Aborting."
            
        logger.info("--- Level 2: for those we send a notification to, see if there are violation records in the past quarter. if there are violation records: include a sample violation.")
        # 2021-08-06: correction for KY, we will disclose a sample violation for permits with known violations in the past quarter
        data = random_assignment.level_two_randomization(df=data, treatment_fraction=1, random_seed=seed)
        assert 'sample_violation_flag' in data.columns.tolist(), "Expect sample_violation_flag to be in the column list of data. Aborting."
        assert sum(data.sample_violation_flag.isna()) == 0, "Expect every permit to have an assignment. Aborting."

        logger.info("--- Level 3: within the facilities for which we will include a sample violation, if the permit has more than one problematic pollutant, we will randomize which pollutant to show in the email")
        data = random_assignment.level_three_randomization(df=data, random_seed=seed)
        assert 'sample_pollutant_flag' in data.columns.tolist(), "Expect sample_pollutant to be in the column list. Aborting"
        assert sum(data.sample_pollutant_flag.isna()) == 0, "Expect every permit to have an assignment. Aborting."
        assert sum(data[data.sample_violation_flag].selected_violation.isna()) == 0, "Expect permit with sample_violation_flag on to have a selected violation to show. Aborting."

        logger.info('We have {} at risk permits in total.'.format(len(data)))
        logger.info("We will send a notification to {} permits.".format(sum(data.notification_flag)))
        logger.info("We will include a sample violation for {} permits.".format(sum(data.sample_violation_flag)))
        logger.info("We will randomize disclosure of pollutant for {} permits.".format(sum(data.sample_pollutant_flag)))

        logger.info("====== 3/6 Save at risk permit data and treatment assignment to database ======")
        data['file_timestamp'] = runtime
        sql_save.save_at_risk_permits_assignment(mode=mode, data=data, engine=engine)
            
        logger.info('===== 4/6 Generate emails with at-risk permits data and email templates =====')
        # subset to permits that we will send the notifications to
        notif_group = data[data.notification_flag]
        data_dict = notif_group.to_dict('records')
        database_update_date_lst = [database_update_date]*len(data_dict)
        email_dicts = list(map(templating.generate_email_dict, data_dict, database_update_date_lst))

        logger.info("====== 5/6 Saving email info to json files ======")
        json_functions.save_emails_to_json(email_dicts, emails_dir=oak_emails_dir)

        logger.info('======= 6/6 Saving log file and sync to s3 bucket =======')
        logger.info(f'Script FINISHED. Log file saved in {oak_log_dir} and all project files synced with S3 bucket.')
        with open(os.path.join(oak_log_dir, 'email_maker.log'), 'w') as file:
            file.write(log_capture_string.getvalue())

        subprocess.run(['aws', 's3', 'sync', oak_project_dir, s3_project_dir], check=True)

        # if mode is prod, we copy the folder from prod to test folder for testing emails on Whippet
        # this command will copy all folders in prod to test
        if mode == 'prod':
            print('Mode is production, saving folders in prod to test.')
            os.system(f'aws s3 cp {s3_project_dir}/prod/ {s3_project_dir}/test/ --recursive')

        print('===== Finish running email maker =====')
    else: 
        print('Script discontinued by user. Exiting.')

if __name__ == "__main__":
    """See https://stackoverflow.com/questions/419163/what-does-if-name-main-do"""
    main()
