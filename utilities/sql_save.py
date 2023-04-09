"""
Module to save or push data to the database
"""
import pandas as pd

from io import StringIO
import csv

def psql_insert_copy(table, conn, keys, data_iter):
    """
    Execute SQL statement inserting data

    Parameters
    ----------
    table : pandas.io.sql.SQLTable
    conn : sqlalchemy.engine.Engine or sqlalchemy.engine.Connection
    keys : list of str
        Column names
    data_iter : Iterable that iterates the values to be inserted

    Returns
    --------
    None

    References
    --------
    https://stackoverflow.com/questions/23103962/how-to-write-dataframe-to-postgres-table and
    https://pandas.pydata.org/pandas-docs/stable/user_guide/io.html#io-sql-method
    """
    # gets a DBAPI connection that can provide a cursor
    print('Using PSQL Fast Copy')
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        s_buf = StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)

        columns = ', '.join('"{}"'.format(k) for k in keys)
        if table.schema:
            table_name = '{}.{}'.format(table.schema, table.name)
        else:
            table_name = table.name

        sql = 'COPY {} ({}) FROM STDIN WITH CSV'.format(
            table_name, columns)
        cur.copy_expert(sql=sql, file=s_buf)

def save_at_risk_permits_assignment(mode, data, engine=None):
    """
    Append passed df to aws table: esnc_risk_notif.at_risk_permits
    - in prod mode, check that the quarter doesnt exist, and then append
    - in test mode with a test database mimicking prod, overwrite the table

    Parameters
    --------
    mode: character 
        define the mode of code running: prod or test
    data: a pandas dataframe
        with at risk permits data and treatment assignemnt
    engine: postgresql database connection 
    test_data_dir: character
        path to test data 

    Returns
    --------
    None
    """
    if mode == 'prod':
        with engine.begin() as conn:
            table_exist = pd.read_sql("""
            SELECT
              EXISTS (
                SELECT
                FROM
                  information_schema.tables
                WHERE
                  table_schema = 'esnc_risk_notif'
                  AND table_name = 'at_risk_permits'
              );
            """, conn).exists[0]
            if table_exist:
                existing_df = pd.read_sql("SELECT distinct fiscal_quarter FROM esnc_risk_notif.at_risk_permits", conn)
                existing_quarters = existing_df.fiscal_quarter.tolist()
                assert data.fiscal_quarter[0] not in existing_quarters, "{} already exists in esnc_risk_notif.at_risk_permits. Aborting.".format(data.fiscal_quarter[0])
            data.to_sql(
                name='at_risk_permits',
                con=conn,
                schema='esnc_risk_notif',
                if_exists='append',
                index=False,
                method=psql_insert_copy
            )
    elif mode == 'test':
         with engine.begin() as conn:
            data.to_sql(
                name='esnc_notif_at_risk_permits',
                con=conn,
                schema='sandbox',
                if_exists='replace',
                index=False,
                method=psql_insert_copy
            )    
    else:
        raise Exception("mode not matched prod or test")

def save_batch_report(mode, batch_report, engine=None):
    """
    Append passed df to aws table: esnc_risk_notif.batch_report

    Parameters
    ---------
    mode: string
        define the mode of code running: prod or test
    batch_report: dataframe
        batch report from whippet sender run
    engine: postgresql database connection

    Returns
    --------
    None
    """
    # in production mode, check if the run_id already exists. If exists - abort, if not, create/ append
    if mode == 'prod':
        with engine.begin() as conn:
            table_exist = pd.read_sql("""
            SELECT
              EXISTS (
                SELECT
                FROM
                  information_schema.tables
                WHERE
                  table_schema = 'esnc_risk_notif'
                  AND table_name = 'batch_report'
              );
            """, conn).exists[0]
            if table_exist:
                existing_df = pd.read_sql("SELECT distinct run_id FROM esnc_risk_notif.batch_report", conn)
                existing_run_ids = existing_df.run_id.tolist()
                assert batch_report.run_id[0] not in existing_run_ids, "{} already exists in esnc_risk_notif.batch_report. Aborting."
            batch_report.to_sql(
                    name='batch_report',
                    con=conn,
                    schema='esnc_risk_notif',
                    if_exists='append',
                    index=False,
                    method=psql_insert_copy
            )
    # in test mode, replace table if it already exists
    elif mode == 'test':
        with engine.begin() as conn:
            batch_report.to_sql(
                name='esnc_notif_batch_report',
                con=conn,
                schema='sandbox',
                if_exists='replace',
                index=False,
                method=psql_insert_copy
            )
    else:
        raise Exception("mode not matched prod or prodtest")

