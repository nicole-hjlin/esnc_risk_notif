"""
Generate emails with templates. 
"""

from .templates import default
from .templates import kentucky

def generate_email_dict(info_dict, database_update_date):
    """
    Generate an email data dictionary with only the key elements from info_dict. 
    
    Parameters
    --------
    info_dict: a dictionary object
        must contains npdes_permit_id, fiscal_quarter, recipient email addresses, cc_addresses, (optional) bcc_addresses
    database_update_date: string
        the most recent update date for RegLab's AWS databse in YYYY-MM-DD format. 
   
    Returns
    --------
    emaill_dict: a dictionary object
        contains the key elements from info_dict and generated email template from templating.

    Dependencies
    --------
    templating.templates
    """
    # make sure necessary columns are present
    assert set(['npdes_permit_id', 'permit_name', 'permit_state', 'location_address', 'fiscal_quarter', 'to_email_addresses', 'sample_violation_flag', 'selected_violation', 'sample_pollutant_flag']).issubset(set(info_dict.keys())), "Expect info_dict to contain columns 'npdes_permit_id', 'permit_name', 'permit_state', 'location_address', 'fiscal_quarter', 'to_email_addresses', 'sample_violation_flag', 'selected_violation', 'sample_pollutant_flag', all necessary for generating emails. Aborting."

    state = info_dict['permit_state']
    if state == 'KY':
        template_identifier, subject, header, body = kentucky.template(info_dict)
    else: 
        template_identifier, subject, header, body = default.template(info_dict, database_update_date)

    email_dict = {'npdes_permit_id': info_dict['npdes_permit_id'], 
                  'fiscal_quarter': info_dict['fiscal_quarter'], 
                  'to_addrs': info_dict['to_email_addresses'], 
                  'bcc_addrs': 'hongjinl@law.stanford.edu', 
                  'email_template': template_identifier,
                  'subject': subject,
                  'header': header, 
                  'body': body}     

    return email_dict