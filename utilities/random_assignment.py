"""
Module for randomization scheme.

Assumming 100 at-risk facilities for simplication:
Level 1: randomly split into 50-50. We will send notifications to 50 facilities, 
Level 2: within the 50 facilities that have violations in the past six months, if the permit has one problematic pollutant, we show the violation of that pollutant with the highest exceedance percentage.
Level 3: within the facilities for which we will include a sample violation and have more than one problematic pollutant, we will randomize which pollutant to show in the email.

In this design, we can answer two different research questions: 
Level 1: Does a notification have an impact on reducing non-compliance rate?
Level 3: Do facilities pay more attention to the pollutant mentioned in the notification than pollutants not mentioned?
"""

import pandas as pd
import numpy as np
import random
import datetime as dt

# ----- utility functions
def randomize_permits(permit_list, treatment_fraction=0.5, random_seed=333):
    """
    take a list of permit ids and return a dataframe with two columns, permit ids and boolean assignments of treatment (True) and control (False)
    
    Parameters
    --------
    permit_list: list
        a list of permit ids
    treatment_fraction: numeric
        the proportion of permits in the positive class (treatment group)
    random_seed: integer
        the random seed that controls reproducibility 
        
    Returns
    -------
    assignment: pandas dataframe
        with two columns, permit ids and assignments
    """
    assert type(permit_list) == list, "expect a list of keys (permit ids)"
    assert treatment_fraction <= 1, "ensure we don't accidentally use an integer percent"
    n = len(permit_list)
    binary_assignments = np.ones(n) #set all True
    binary_assignments[:int(n*(1-treatment_fraction))] = 0  #set eg. half to False
    
    np.random.seed(random_seed)
    np.random.shuffle(binary_assignments)
    bool_assignments = binary_assignments.astype(bool)
    assignment = pd.DataFrame({'npdes_permit_id': permit_list, 
                               'assignment': bool_assignments})

    return assignment


def get_viol_past_quarter(historical_violations, quarter_start_date):
    """
    subset to violation records in the past quarter given a list of known violations as dictionaries and the prediction quarter start date
    
    Parameters
    ---------
    historical_violations: list 
        a list of all historical effluent violations as dictionary objects, assuming key 'monitoring_period_end_date' with values formatted as 'YYYY-MM-DD'.
    quarter_start_date: datetime date object 
        the start date of the prediction quarter, e.g. '2021-01-01' for FY 2021Q2 
    
    Returns
    ---------
    viol_past_quarter: list
        a list of effluent violations in the past quarter as dictionary objects
    """
    previous_quarter_start_date_timestamp = pd.Timestamp(quarter_start_date) - pd.DateOffset(months=6)
    previous_quarter_start_date = pd.to_datetime(previous_quarter_start_date_timestamp)
    quarter_start_date = pd.to_datetime(quarter_start_date)
    viol_past_quarter = []
    if historical_violations is not None:
        for v in historical_violations:
            monitoring_period_end_date = dt.datetime.strptime(v['monitoring_period_end_date'], '%Y-%m-%d').date()
            if previous_quarter_start_date < monitoring_period_end_date < quarter_start_date:
                viol_past_quarter.append(v)
    
    return viol_past_quarter

def rank_param_from_viol(viol_list):
    """
    rank parameters by exceedance percentages given a list of violations as dictionary objects (in descending order).
    
    Parameters
    -------
    viol_list: list 
        a list of effluent violations as dictionary objects, which should contain keys 'parameter_description' and 'exceedence_percentage'
    
    Returns
    -------
    param_list_ordered: list
        an ordered list of parameters based on exceedence percentages (in descending order)
    """
    if viol_list != []: 
        viol_df = pd.DataFrame(viol_list)
        viol_df = viol_df.sort_values(by = 'exceedence_percentage', ascending = False)
        param_df_ordered = viol_df.groupby(['parameter_description'])['exceedence_percentage'].agg(['max']).sort_values('max', ascending = False)
        param_list_ordered =  list(param_df_ordered.index.get_level_values('parameter_description'))
    else: 
        param_list_ordered = []

    return param_list_ordered

def sample_param(param_list_ordered, random_seed):
    """
    randomly select one of the first two parameters from a list of parameters, assuming an ordered list 
    
    Parameters
    --------
    param_list_ordered: list
        a list of parameters as strings
    random_seed: integer
        an integer that set the random seed 
    
    Returns
    --------
    treatment_pollutant: string
        the string of the treatment (selected) pollutant
    control_pollutant: string
        the string of the control (unselected) pollutant
    """
    if param_list_ordered != []:
        if len(param_list_ordered) > 1:
            rct_param = param_list_ordered[:2]
            random.seed(random_seed)
            treatment_pollutant = random.choice(rct_param)
            control_pollutant = [p for p in rct_param if p != treatment_pollutant][0]
        else: 
            treatment_pollutant = param_list_ordered[0]
            control_pollutant  = ""
    else:
        treatment_pollutant = ""
        control_pollutant  = ""

    return treatment_pollutant, control_pollutant 

def select_violation(viol_list, param):
    """
    select the top effluent violation with the highest exceedence percentage from a list of violations given a parameter, assuming that the violation list contains the parameter. 
    
    Parameters
    -------
    viol_list: list
        a list of effluent violations as dictionary objects, which should contain keys 'parameter_description' and 'exceedence_percentage'
    param: string
        a string describing the pollutant 
    
    Returns
    -------
    selected_viol: list
        a list of a dictionary object of the selected violation
    """
    if viol_list != [] and param != "":
        param_viol = []
        for v in viol_list:
            if v['parameter_description'] == param:
                param_viol.append(v)
        assert param_viol != [], "The violation list does not contain the pollutant."
        param_viol_ordered = sorted(param_viol, key = lambda i: i['exceedence_percentage'], reverse = True)
        selected_viol = param_viol_ordered[0]
    else:
        selected_viol = None
    # save in a list object to match what templating.template.py expects
    selected_viol = [selected_viol]

    return selected_viol


# --- main functions
def level_one_randomization(df, treatment_fraction=0.5, random_seed=333):
    """
    implement level one randomization: split permit list into a treatment and a control group. We will send notifications to permits in the treatment group.

    Parameters:
    ---------
    df: pandas dataframe
        the dataframe returned from querying sql_queries.at_risk_permits. It must has a column for npdes_permit_id
    treatment_fraction: numeric
        the proportion of permits in the positive class (treatment group). Feed into the 
    random_seed: integer
        the random seed that controls reproducibility 

    Returns:
    ---------
    df: pandas dataframe 
        mutated original data with a new column notification_flag: if true, we will send the notification to
    
    Dependencies:
    ---------
    randomize_permits
    """
    # make sure the columns exist 
    assert 'npdes_permit_id' in df.columns.tolist(), "expect npdes_permit_id to be in the column list of data."
    
    permit_list = df['npdes_permit_id'].tolist()
    assignment = randomize_permits(permit_list, treatment_fraction=treatment_fraction, random_seed=random_seed)
    assignment = assignment.rename(columns = {'assignment': 'notification_flag'})
    df = df.merge(assignment, on = 'npdes_permit_id', how = 'left')

    return df

def level_two_randomization(df, treatment_fraction=0.5, random_seed=333):
    """
    implement level two randomization: for notified permits with violation records in the past quarter, further split into a treatment and a control group depending on the treatment fraction. We will include a sample violation in the email sent to the treatment group.  If treatment fraction = 1, we are not randomizing. Default treatment fraction = 0.5
    it should only be run after level_one_randomization has been implemented

    Parameters:
    ---------
    df: pandas dataframe
        the dataframe returned from querying sql_queries.at_risk_permits. It must has columns: npdes_permit_id, notification_flag, known_violations, and calendar_quarter_start_date
    treatment_fraction: numeric
        the proportion of permits in the positive class (treatment group).
    random_seed: integer
        the random seed that controls reproducibility 

    Returns:
    ---------
    df: pandas dataframe 
        mutated original data with a new column sample_violation_flag: if true, we will include a sample violation in the email.

    Dependencies:
    ---------
    get_viol_past_quarter, randomize_permits
    """
    # make sure the columns exist 
    assert 'npdes_permit_id' in df.columns.tolist(), "expect npdes_permit_id to be in the column list of data."
    assert 'notification_flag' in df.columns.tolist(), "expect notification_flag to be in the column list of data, i.e. level_one_randomization has to be run first."
    assert 'known_violations' in df.columns.tolist(), "expect known_violations to be in the column list of data."
    assert 'calendar_quarter_start_date' in df.columns.tolist(), "expect calendar_quarter_start_date to be in the column list of data."

    # get violations in the past quarter (consider separating this out.)
    def df_get_viols(df):
        f = lambda row : get_viol_past_quarter(row.known_violations, row.calendar_quarter_start_date)
        return df.apply(f, axis=1)

    df['viol_past_quarter'] = df_get_viols(df)
    df['has_viol_past_quarter_flag'] = [len(viol) > 0 for viol in df['viol_past_quarter']]
    # subset to permits that we will send a notification to and has violation records in the past quarter
    sub_data = df[(df.notification_flag) & (df.has_viol_past_quarter_flag)]
    # randomize half to include a sample violation
    permit_list = sub_data['npdes_permit_id'].tolist()
    assignment = randomize_permits(permit_list, treatment_fraction = treatment_fraction, random_seed = random_seed)
    assignment = assignment.rename(columns = {'assignment': 'sample_violation_flag'})
    df = df.merge(assignment, on = 'npdes_permit_id', how = 'left')
    df.sample_violation_flag = df.sample_violation_flag.fillna(False)

    return df

def level_three_randomization(df, random_seed=333):
    """
    implement level three randomization: for notified permits with sample violations and more than one pollutant triggering violations in the past quarter, we randomly select one of the top two pollutants ranked by exceedance percentage to disclose in the email.
    it should only be run after level_one_randomization and level_two_randomization have been run

    Parameters:
    --------
    df: pandas dataframe
        the dataframe returned from querying sql_queries.at_risk_permits. It must has columns: npdes_permit_id, sample_violation_flag, and viol_past_quarter
    random_seed: integer
        the random seed that controls reproducibility 

    Returns:
    --------
    df: pandas dataframe 
        mutated original data with new columns:
        - sample_pollutant_flag: if true, we will sample a pollutant for this permit.
        - random_seed: the random seed specific to the permit for selecting the pollutant
        - treatment_pollutant: the pollutant that we will disclose in the email
        - control_pollutant: the other pollutant that we will not disclose in the email. this will only have value if the sample_pollutant_flag is true.
        - selected_violation: the final sample violation to disclose in the email
    
    Dependencies:
    ---------
    rank_param_from_viol, sample_param, and select_violation
    """
    # make sure the columns exist 
    assert 'npdes_permit_id' in df.columns.tolist(), "expect npdes_permit_id to be in the column list of data."
    assert 'sample_violation_flag' in df.columns.tolist(), "expect sample_violation_flag to be in the column list of data, i.e. level one and level two randomization has to be run first."
    assert 'viol_past_quarter' in df.columns.tolist(), "expect viol_past_quarter to be in the column list of data, i.e. level two randomization has to be run first."
    
    sub_data = df[df.sample_violation_flag].copy()
    random.seed(random_seed)
    sub_data['random_seed'] = random.sample(range(1, 10000), len(sub_data))
    # get the ordered list of problematic pollutants in the past quarter
    sub_data['param_past_quarter_ordered'] = sub_data.viol_past_quarter.apply(rank_param_from_viol)
    # get treatment and control pollutant from ordered problematic parameter list
    sub_data['treatment_pollutant'], sub_data['control_pollutant'] = np.vectorize(sample_param)(sub_data.param_past_quarter_ordered, sub_data.random_seed)
    # select violation for treatment pollutant
    sub_data['selected_violation'] = np.vectorize(select_violation)(sub_data.viol_past_quarter, sub_data.treatment_pollutant)
    # create flag for facilities where we sample pollutants
    sub_data['sample_pollutant_flag'] = [viol != "" for viol in sub_data.control_pollutant]
    df = df.merge(sub_data[['npdes_permit_id', 'random_seed', 'param_past_quarter_ordered', 'treatment_pollutant', 'control_pollutant', 'selected_violation', 'sample_pollutant_flag']], on = 'npdes_permit_id', how = 'left')
    df.sample_pollutant_flag = df.sample_pollutant_flag.fillna(False)

    return df
 