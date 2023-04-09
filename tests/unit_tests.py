"""
Unit tests for selected functions from module:
- utilities/random_assignment.py
- utilities/data_getting/sql_grab.py
"""

import .utilities.data_getting.sql_grab
import .utilities.random_assignment

# ---- sql_grab
def test_get_data_base_update_date():
    """
    unit test for get_data_base_update_date()
    """
    mode = 'prodtest'
    engine = ENGINE
    d = sql_grab.get_database_update_date(mode, engine)
    assert isinstance(d, datetime.date), 'Returned variable is not a datetime.date object.'
    
def test_get_users():
    """
    unit test for get_users()
    """
    mode = 'prodtest'
    engine = ENGINE

    df = sql_grab.get_users(mode, engine)

    assert type(df) == pd.DataFrame, "Returned object is not a pandas dataframe."
    assert df.columns.tolist() == ['npdes_permit_id', 'user_email'], 'Returned dataframe does not have the right columns.'
    
def test_get_at_risk_permits():
    """
    unit test for get_at_risk_permits
    
    Since we are directly reading from the data set, this "unit test" might take a while.
    """
    mode = 'prodtest'
    model_id = 'notification_intervention_2021q3_2021-04-28_144616_986680'
    states_list = ['MD', 'KY', 'CO', 'TN']
    classification_threshold = 0.5
    engine = ENGINE
    df = sql_grab.get_at_risk_permits(mode, model_id, states_list, classification_threshold, engine)
    assert len(df) > 0, "No data returned. Check query."

# ----- random_assignment
def test_randomize_permits():
	"""
	unit test for randomize_permits()
	"""
	permit_list = ['a', 'b', 'c', 'd']
    assignment = random_assignment.randomize_permits(permit_list, treatment_pct=0.5, random_seed=333)
    assert type(assignment) == pd.DataFrame, "expect output to the a pandas dataframe"
    assert assignment.columns.tolist() == ['npdes_permit_id', 'assignment'], "expect output to have two columns with names ['npdes_permit_id', 'assignment']."
    assert assignment.npdes_permit_id.tolist() == permit_list, "expect npdes_permit_id to match the permit_list."
    assert assignment.assignment.tolist() == [True, False, True, False], "expect assignment to be T, F, T, F using seed 333."

def test_get_viol_past_quarter():
	"""
	unit test for get_viol_past_quarter()
	"""
	historical_violations = [{'monitoring_period_end_date': '2020-01-01'},
							 {'monitoring_period_end_date': '2020-09-10'},
							 {'monitoring_period_end_date': '2020-11-23'},
							 {'monitoring_period_end_date': '2021-02-21'}]
	quarter_start_date = dt.datetime.strptime('2021-01-01', '%Y-%m-%d').date()
	viol_past_quarter = random_assignment.get_viol_past_quarter(historical_violations, quarter_start_date)
	assert viol_past_quarter == [{'monitoring_period_end_date': '2020-11-23'}], 'get_viol_past_quarter failed.'

def test_rank_param_from_viol():
	"""
	unit test for rank_param_from_viol()
	"""
	viol_list = [{'parameter_description': 'a', 'exceedence_percentage': 10},
				 {'parameter_description': 'a', 'exceedence_percentage': 20},
				 {'parameter_description': 'b', 'exceedence_percentage': 30},
				 {'parameter_description': 'b', 'exceedence_percentage': 40},
				 {'parameter_description': 'c', 'exceedence_percentage': 50}]
	param_list_ordered = random_assignment.rank_param_from_viol(viol_list)
	assert param_list_ordered == ['c', 'b', 'a'], 'rank_param_from_viol failed.'

def test_sample_param():
	"""
	unit test for sample_param()
	"""
	param_list_ordered = ['a', 'b', 'c']
	random_seed = 3
	treatment_pollutant, control_pollutant = random_assignment.sample_param(param_list_ordered, random_seed)
	# tested with random seed 3
	assert (treatment_pollutant, control_pollutant) == ('a', 'b'), 'sample_param failed'

def test_select_violation():
	"""
	unit test for select_violation()
	"""
	viol_list = [{'parameter_description': 'a', 'exceedence_percentage': 10},
				 {'parameter_description': 'a', 'exceedence_percentage': 20},
				 {'parameter_description': 'b', 'exceedence_percentage': 30},
				 {'parameter_description': 'b', 'exceedence_percentage': 40},
				 {'parameter_description': 'c', 'exceedence_percentage': 50}]
	param = 'a'
	selected_viol = random_assignment.select_violation(viol_list, param)
	assert selected_viol == {'parameter_description': 'a', 'exceedence_percentage': 20}, 'select_violation failed'

# ------ quarter
def usage_example_and_test():
	"""
	unit test for utilities.quarter
	"""
    my_date_string = '01/01/2021 00:00:00'
    my_date = dt.datetime.strptime(my_date_string, '%d/%m/%Y %H:%M:%S')
    quarteryear = quarter.quarter_year(my_date)
    assert quarteryear == (2, 2021)
    prev_quarteryear = quarter.prev_quarter(quarteryear[0], quarteryear[1])
    assert prev_quarteryear == (1, 2021)
    assert quarter_year_string(quarteryear[0], quarteryear[1]) == '2021Q2'
    
def get_month_quarter_test():
	"""
	unit test for utilities.quarter
	"""
    all_months = [1,2,3,4,5,6,7,8,9,10,11,12]
    result_quarters = [quarter.quarter_for_month(m) for m in all_months]
    desired_quarters = [2,2,2, 3,3,3 ,4,4,4, 1,1,1]
    pairs = zip(desired_quarters, result_quarters)
    print(list(pairs))
    assert all([desired==result for desired, result in pairs])
    

