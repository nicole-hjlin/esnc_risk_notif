# ESNC Risk Notification Emailer System 

This repository contains code to generate and send emails to at-risk facilities determined by a machine learning model. Before this point, we would have to run the machine learning model first. The machine learning model is housed in this [repo](https://github.com/reglab/epa_risk/tree/master) and described in this [confluence page](https://asconfluence.stanford.edu/confluence/display/REGLAB/Data+Pipeline). 

For general project description and FAQ see this [google doc](https://docs.google.com/document/d/1mx-i_0a2KY4X6nqxCA9HUQUhB53JAGU0qylhMt8-Bwk/edit). For all other project related documentation see this [Confluence page](https://asconfluence.stanford.edu/confluence/display/REGLAB/ESNC+Risk+Notification+Intervention). 

## Setup

Scripts in this repository are written in Python 3.7, assuming scripts executed from the repo root directory. 

### Environment 
We control dev environment for Python using Singularity on Sherlock. 

To access the container and run python files within the image, ssh into Sherlock and run
```
singularity shell --nv $GROUP_HOME/singularity/epa_7-08-21_2.sif
```
To run pythn files directly on sbatch jobs or your login node, ssh into Sherlock and run
```
singularity exec $GROUP_HOME/singularity/epa_7-08-21_2.sif python3 -m your_py_file --arguments
```

For getting dev environment setup for Python on your local machine, see the instructions below:
Install pyenv virtual env
```
brew install pyenv-virtualenv
```

Add the following lines to your `~/.bash_profile` file
```
if which pyenv > /dev/null; then eval "$(pyenv init -)"; fi                                       
 if which pyenv-virtualenv-init > /dev/null; then
     eval "$(pyenv virtualenv-init -)";
fi
```

Install python
```
pyenv install 3.7.4
```

Create a new virtual environment and activate
```
pyenv virtualenv 3.7.4 esnc_risk_notif
pyenv activate esnc_risk_notif
```

Install python packages
```
pip install -r requirements.txt
```


### Code Structure
``` 
├── 0_email_maker.py
├── 1_whippet_sender.py
├── 2_batch_report_sync.py
├── README.md
├── configs.py
├── requirements.txt
├── tests
│   ├── line_by_line_test_batch_report_sync.ipynb
│   ├── line_by_line_test_email_maker.ipynb
│   ├── line_by_line_test_whippet_sender.ipynb
│   └── unit_tests.py
└── utilities
    ├── json_functions.py
    ├── random_assignment.py
    ├── sql_grab.py
    ├── sql_queries.py
    ├── sql_save.py
    ├── templates
    │   ├── default.py
    │   └── kentucky.py
    └── templating.py
```

## Replication Steps 
There are six main stages to this emailing system: 
1. pull at-risk permits data from RegLab's AWS database, which houses the model outputs. 
2. implement randomization to determine 1) which permit to send the notification to, 2) which notification to include a sample violation, and 3) which pollutant to disclose in the email. 
3. construct emails using state-specific templates 
4. upload emails to EPA's production environment, whippet
5. send emails from whippet and generate batch reports
6. download batch reports from whippet to RegLab's Sherlock OAK folder and AWS database 

The main scripts that implement the above steps are: 
- `code/0_email_maker.py`
    - This script implements steps 1 - 4 and should be run within Stanford's Sherlock environment.
    - It depends on the following supporting modules: 
        - `configs`: Global variables and configuration functions.
        - `utilities.sql_grab`: Functions for getting info from the database.
        - `utilities.sql_queries`: SQL queries for getting data from the database.
        - `utilities.templating`: Email templates for different states and functions to put info into templates.
        - `utilities.random_assignment`: Functions to implement randomization scheme.
- `code/1_whippet_sender.py`
    - This script implements step 5 and should be run within EPA's production environment.
- `code/2_batch_report_finalizer.py` 
    - This script implements step 6 and should be run within Stanford's Sherlock environment.
