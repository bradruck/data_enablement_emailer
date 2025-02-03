# Data Enablement - Data Append, TURN-Weekly Emailer

# Description -
# The Data Enablement Emailer is an automation for the weekly requirement of sending emails informing the availability
# of a zip file on an sFTP server. The email is sent on a weekly basis at the conclusion of an automatic data license
# run from the previous week. The automation is scheduled to run via an ActiveBatch trigger launched via a text-file
# drop into a designated folder located at /zfs1/Operations_limited/Data_Enablement/Data_License_Turn/Trigger_Email/.
#
#
#
# The automation begins by conducting a JIRA search for sub-task (child) tickets that are created from parent TURN
# Tickets. The found tickets are mined for information required for the email generation.  There is also an excel file
# located on ZFS1 that is accessed for email data and all information is copied into an email template that is then
# transmitted in text form. An email text-file is added to the Jira ticket as an attachment and a comment is also posted
# in the ticket alerting 'Revenue Recognition' that the attachment now exists. Finally the 'labels' field is updated to
# indicate that this ticket has been processed to avoid duplicate runs.
# Automation now includes the ability to post zip files to the customer's ftp sight. An option of running without the
# ftp posting is set in the config file under the 'Project Details' section as 'running mode', set as follows:
# 1 -> sftp only, 2 -> email only, 3 -> both sftp and email
#
# Application Information -
# Required modules:     main.py,
#                       data_enablement_email_manager.py,
#                       jira_manager.py,
#                       sftp_manager.py,
#                       email_manager.py,
#                       excel_manager.py,
#                       config.ini
# Deployed Location:    //prd-use1a-pr-34-ci-operations-01/home/bradley.ruck/Projects/data_enablement_emailer/
# ActiveBatch Trigger:  //onlinemodelingdev/Jobs, Folders & Plans/Report/DE_Email/
# Source Code:          //gitlab.oracledatacloud.com/odc-operations/DE_TURN_FTP-Emailer/
# LogFile Location:     //zfs1/Operations_limited/Data_Enablement/Data_License_Turn/Logs_Email/
# ExcelFile Location:   //zfs1/Operations_limited/Data_Enablement/Data_License_Turn/Turn_Doc/
#
# Contact Information -
# Primary Users:        Data Enablement
# Lead Customer:        Zack Batt (zack.batt@oracle.com)
# Lead Developer:       Bradley Ruck (bradley.ruck@oracle.com)
# Date Launched:        February, 2018 as DE_Turn_Emailer
# Date Updated:         January, 2019

# main module
# Responsible for reading in the basic configurations settings, creating the log file, and creating and launching
# the Data Enablement Email Manager (DEEM), finally it launches the purge_files method to remove log files that are
# older than a prescribed retention period
# Updated to include the option of running a console logger for development purposes, bypassed in production
#
from datetime import datetime, timedelta
import os
import configparser
import logging

from VaultClient3 import VaultClient3 as VaultClient
from data_enablement_email_manager import DataEnablementEmailManager


# Define a console logger for development purposes
#
def console_logger():
    # define Handler that writes DEBUG or higher messages to the sys.stderr
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    # set a simple format for console use
    formatter = logging.Formatter('%(levelname)-7s: %(name)-30s: %(threadName)-12s: %(message)s')
    console.setFormatter(formatter)
    # add the Handler to the root logger
    logging.getLogger('').addHandler(console)


def main(con_opt='n'):
    today_date = (datetime.now() - timedelta(hours=6)).strftime('%Y%m%d-%H%M%S')

    # create a configparser object and open in read mode
    config = configparser.ConfigParser()
    config.read('config.ini')

    # Vault Client Objects
    VC_Obj = VaultClient("prod")
    jira_pd = VC_Obj.VaultSecret('jira', str(config.get('Jira', 'authorization')))
    ssh_key = VC_Obj.VaultSecret('ssh', str(config.get('sFTP', 'authorization')))

    # create a dictionary of configuration parameters
    config_params = {
        "email_file_name":          config.get('Project Details', 'file_name'),
        "running_mode":             config.get('Project Details', 'running_mode'),
        "sftp_server":              config.get('Project Details', 'sftp'),
        "jira_url":                 config.get('Jira', 'url'),
        "jira_token":               tuple([config.get('Jira', 'authorization'), jira_pd]),
        "jql_status_parent":        config.get('Jira', 'status_parent'),
        "jql_status_child_sftp":    config.get('Jira', 'status_child_sftp'),
        "jql_status_child_email":   config.get('Jira', 'status_child_email'),
        "jql_issuetype":            config.get('Jira', 'issuetype'),
        "jql_label":                config.get('Jira', 'label'),
        "jql_text":                 config.get('Jira', 'text'),
        "ssh_key":                  ssh_key,
        "sftp_url":                 config.get('sFTP', 'url'),
        "sftp_user":                config.get('sFTP', 'user'),
        "sftp_path_to_keyfile":     config.get('sFTP', 'path_to_keyfile'),
        "sftp_folder_path":         config.get('sFTP', 'ftp_folder_path'),
        "sftp_zip_file_path":       config.get('sFTP', 'zip_file_path'),
        "excel_path":               config.get('ExcelFile', 'path'),
        "email_subject":            config.get('Email', 'subject'),
        "email_to":                 config.get('Email', 'to'),
        "email_from":               config.get('Email', 'from'),
        "email_cc":                 config.get('Email', 'cc')
    }

    # logfile path to point to the Operations_limited drive on zfs
    purge_days = config.get('LogFile', 'retention_days')
    log_file_path = config.get('LogFile', 'path')
    logfile_name = '{}{}_{}.log'.format(log_file_path, config.get('Project Details', 'app_name'), today_date)

    # check to see if log file already exits for the day to avoid duplicate execution
    if not os.path.isfile(logfile_name):
        logging.basicConfig(filename=logfile_name,
                            level=logging.INFO,
                            format='%(asctime)s: %(levelname)-7s:'
                                   ' %(name)-30s: %(threadName)-12s: %(message)s',
                            datefmt='%m/%d/%Y %H:%M:%S',
                            filemode='w')

        logger = logging.getLogger(__name__)

        # this is only enacted if main.py is run as the executable
        if con_opt and con_opt in ['y', 'Y']:
            console_logger()

        logger.info("Process Start - Weekly Email, Data Enablement - {}\n".format(today_date))

        # create DEEM object and launch Email Generator
        de_emailer = DataEnablementEmailManager(config_params)
        de_emailer.process_manager()

        # search logfile directory for old log files to purge
        de_emailer.purge_files(purge_days, log_file_path)


if __name__ == '__main__':
    # prompt user for use of console logging -> for use in development not production
    ans = input("\nWould you like to enable a console logger for this run?\n Please enter y or n:\t")
    print()
    main(ans)
