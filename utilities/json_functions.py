import os
import json
import boto3

def save_emails_to_json(email_dicts, emails_dir):
    """
    save emails as dictionary objects to json files.

    Parameters
    -------
    maildata: a list of dictionary objects 
        with permit id, quarter, email address, cc email address, email subject, and email body
    emails_out_dir: a os path string
        the output folder to store the email json objects 

    Returns 
    -------
    Print message: "Emails saved in {emails_dir}."
    """
    email_count = len(email_dicts)
    for i in range(email_count):
        permit_maildata = email_dicts[i]
        filename = "{}.json".format(permit_maildata['npdes_permit_id'])
        filepath = os.path.join(emails_dir, filename)
        with open(filepath, 'w') as f:
            json.dump(permit_maildata, f, indent=4, sort_keys=True)
            
    return "Emails saved in {}.".format(emails_dir)


def read_emails_from_json(emails_dir, s3=True, bucket=None):
    """
    Read json from the files in emails_dict and return a 2-tuple consisting of filenames (list) and the dicts parsed from json (list).
    
    Parameters
    -------
    emails_dir: string
        directory containing json files
    s3: boolean 
        whether the directory is pointing to an s3 bucket
    bucket: s3 bucket connection
        must supply bucket object if s3 = True
    
    Returns 
    -------
    email_dicts: list
        a list of dictionaries parsed from the json files.
    """
    if s3:
        assert bucket != None, "Expect an s3 bucket connection when s3 is True. Aborting."

        email_dicts = []
        for obj in bucket.objects.filter(Prefix=emails_dir):
            content = obj.get()['Body'].read()
            email_dicts.append(json.loads(content))
    else:
        filenames = os.listdir(emails_dir)
        # only keep json files
        email_jsons = [f for f in filenames if '.json' in f]
        
        email_dicts = []
        for email in email_jsons: 
            with open(os.path.join(emails_dir, email)) as file:
                email_dicts.append(json.load(file))
    
    return email_dicts
