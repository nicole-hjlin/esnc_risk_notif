"""
SQL queries to grab at risk permits data and database update date.
"""

DATABASE_UPDATE_DATE = """
/*
Get the most recent update date of RegLab's AWS database for EPA data
*/

SELECT
  max(file_timestamp) AS database_update_date
FROM
  data_ingest.icis__dmrs
"""

_AT_RISK_PERMITS = lambda model_id, states_string, classification_threshold: f"""
/*
Gets prediction results (with specified model id) and build table for notifications:
1. permit meta info
2. risk scores
3. risk flags
4. number of quarters in ESNC in the past three years
5. number of parameters that already triggered ESNC or are one violation away from triggering ESNC in the past three months
6. historical effluent violations in json format (to be sampled in the runner script)
The runner script should take this table and 
1. sum up the number of parameters that already triggered ESNC and those that are in warning status
2. randomize facilities into treatment and control group
3. sample dmr violations to display in the email
*/
WITH base AS (
  SELECT
    model.npdes_permit_id,
    model.permit_name,
    model.permit_state,
    xref.state_name,
    facilities.location_address,
    calendar_quarter_start_date,
    CASE
      WHEN extract(
        MONTH
        FROM
          calendar_quarter_start_date
      ) BETWEEN 10
      AND 12 THEN extract(
        year
        FROM
          calendar_quarter_start_date
      ) + 1 || 'Q1'
      WHEN extract(
        MONTH
        FROM
          calendar_quarter_start_date
      ) BETWEEN 1
      AND 3 THEN extract(
        year
        FROM
          calendar_quarter_start_date
      ) || 'Q2'
      WHEN extract(
        MONTH
        FROM
          calendar_quarter_start_date
      ) BETWEEN 4
      AND 6 THEN extract(
        year
        FROM
          calendar_quarter_start_date
      ) || 'Q3'
      WHEN extract(
        MONTH
        FROM
          calendar_quarter_start_date
      ) BETWEEN 7
      AND 9 THEN extract(
        year
        FROM
          calendar_quarter_start_date
      ) || 'Q4'
    END AS fiscal_quarter,
    predicted_probability_esnc,
    predicted_esnc_flag,
    -- historical esnc 
    frequent_esnc_flag,
    case when num_quarters_in_esnc = 13 then 12 else num_quarters_in_esnc end as num_quarters_in_esnc, -- temporary: need to edit state_prediction_outputs for future launch
    recent_esnc_flag,
    recent_fiscal_quarters_with_esnc,
    -- esnc parameters past quarter
    triggered_esnc_past_quarter_flag,
    parameter_chronic_violation_count,
    parameter_trc_violation_count,
    esnc_paramters_past_quarter,
    -- warning parameters past quarter
    warning_past_quarter_flag,
    parameter_chronic_warning_count,
    parameter_trc_warning_count,
    warning_parameters_past_quarter
  FROM
    model_outputs.state_prediction_outputs AS model
    LEFT JOIN icis.facilities AS facilities USING (npdes_permit_id)
    LEFT JOIN (
      SELECT
        DISTINCT state_abbreviation,
        state_name
      FROM
        ontologies.epa__xref_states_counties_regions
    ) AS xref ON model.permit_state = xref.state_abbreviation
  WHERE
    model_id = '{model_id}' -- this should take in model id, e.g. notification_intervention_2021-03-31_152406_685686
    AND model.permit_state in {states_string} -- subset to participanting states
    AND predicted_probability_esnc > {classification_threshold} -- subset to at risk facilities defined by a predetermined threshold, e.g. 0.5
),
known_violations AS (
  SELECT
    npdes_permit_id,
    count(*) AS violation_count,
    jsonb_agg(
      known_violations
      ORDER BY
        monitoring_period_end_date
    ) AS known_violations
  FROM
    (
      SELECT
        npdes_permit_id,
        monitoring_period_end_date,
        jsonb_build_object(
          'series_id',
          series_id,
          'parameter_group',
          perm_feature_nmbr || '-' || parameter_code || '-' || monitoring_location_code,
          'parameter_description',
          parameter_desc,
          'paramter_category',
          snc_type_group,
          'monitoring_period_end_date',
          monitoring_period_end_date,
          'limit',
          limit_value_standard_units,
          'dmr_value',
          dmr_value_standard_units,
          'unit',
          standard_unit_desc,
          'exceedence_percentage',
          exceedence_pct
        ) AS known_violations
      FROM
        (
          SELECT
            *
          FROM
            icis.dmrs
            -- get model calendar quarter start date
            LEFT JOIN (
              SELECT
                npdes_permit_id,
                calendar_quarter_start_date
              FROM
                model_outputs.state_prediction_outputs AS model
              WHERE
                model_id = '{model_id}'
            ) AS model_quarter_start_date USING (npdes_permit_id)
        ) AS dmrs
      WHERE
        effluent_violation_flag 
        AND monitoring_period_end_date >= calendar_quarter_start_date - INTERVAL '3 years' -- only grap historical violations in the past three years
        AND snc_type_group IN ('1', '2') -- only consider group I and II pollutants
        AND statistical_base_monthly_avg IN ('A', 'N') -- A: monthly averages; N: non-monthly averages
        AND limit_value_standard_units IS NOT NULL
        AND limit_value_qualifier_code IS NOT NULL
        AND (
          rnc_resolution_code IN ('1', 'A') 
          OR rnc_resolution_code IS NULL
        ) -- only consider unresolved violations, see resolution code here: https://docs.google.com/spreadsheets/d/1CvY2ysfEJs3XfBOZm80Eym8N8kUM5JL0PGQYed0vT0E/edit#gid=472012136
        AND exceedence_pct not in (99999, 2147483650) -- remove known data errors https://echo.epa.gov/help/reports/dfr-data-dictionary
    ) AS raw
  GROUP BY
    npdes_permit_id
),
to_email_addresses AS (
  SELECT
    npdes_permit_id,
    string_agg(user_email, ', ') AS to_email_addresses
  FROM
    netdmr.users
  GROUP BY
    npdes_permit_id
)
SELECT
  *
FROM
  base
  LEFT JOIN known_violations USING (npdes_permit_id)
  LEFT JOIN to_email_addresses USING (npdes_permit_id)
"""

def at_risk_permits(model_id, states_list, classification_threshold):
    """
    query at risk facilities and relevant information (including other risk flags and details) with specified model id (e.g. 'notification_intervention_2021q3_2021-04-28_144616_986680', state abbreviation list (e.g. ['MD, KY, CO, TN']), and a classification threshold (e.g. 0.5)
  
    Parameters
    --------
    model_id: string
       the model id of the model run. The model should be specific to a fiscal quarter. E.g. 'notification_intervention_2021q3_2021-04-28_144616_986680'. Check sherlock OAK or GROUPSCRATCH EPA/Analysis/output/model_objects for details. 
    states_list: list
       a list of state abbreviations, e.g. ['MD', 'KY', 'CO', 'TN']
    classification_threshold: numeric
       the threshold with which we classify at-risk facilities, e.g. 0.5

    Returns
    --------
    a completed query with model_id, states_list, and classification threshold
    """
    joined_states_string = "'" + "', '".join(states_list) + "'"
    enclosed_states_string = '(' + joined_states_string + ')'
    classification_threshold_string = str(classification_threshold)
    query = _AT_RISK_PERMITS(model_id, enclosed_states_string, classification_threshold_string)

    return query
