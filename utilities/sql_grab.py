"""Functions for getting info from the database.

Generally, a function has
-a passed 'mode' string which it uses to decide where to look for data
-a passed engine to run the query on (optional as it's not needed if the mode is 'csv')
-a sql query it will run on the database
-control flow based on mode, eg. changes to the sql query such as subbing in a dummy table instead of the real one.
-parsing whatever's returned, eg. parsing dates to datetime.

Heftier sql queries are imported from sql_queries.
"""

import os

import pandas as pd
import datetime as dt
import sqlalchemy as sa

from . import sql_queries 

def get_database_update_date(engine): 
    """
    Get database update date.

    Parameters 
    ---------
    engine: sqlalchemy connection object
        with which we use to connect to RegLab's AWS database

    Returns 
    ---------
    update_date: string
        the dateabase update date in format YYYY-MM-DD.
    """
    sql = sql_queries.DATABASE_UPDATE_DATE 
    with engine.begin() as conn:
        df = pd.read_sql(sql, conn)

    update_date = str(df.database_update_date[0])[:10]

    return update_date

def get_at_risk_permits(model_id, states_list, classification_threshold, engine):
    """
    Get at-risk permits data from AWS database.

    Parameters
    ---------
    model id: character
        the id of the model that we will grab the results from. e.g. 'notification_intervention_2021q3_2021-04-28_144616_986680'
    states_list: list
        a list of state abbreviations
    classification_threshold: numeric
        a number between 0 - 1 with which we use to classify at-risk permits
    engine: sqlalchemy connection object
        with which we use to connect to RegLab's AWS database

    Returns
    --------
    df: pandas dataframe
        results of the sql query 

    Dependencies
    --------
    sql_queries.at_risk_permits

    """
    query = sql_queries.at_risk_permits(model_id, states_list, classification_threshold)  
    with engine.begin() as conn:
        df = pd.read_sql(query, conn) 
   
    df['calendar_quarter_start_date'] = pd.to_datetime(df['calendar_quarter_start_date'], infer_datetime_format=True)
    
    return df
