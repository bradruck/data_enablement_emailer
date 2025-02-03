# data_enablement_email_manager module
# Module holds the class => DataEnablementEmailManager - manages the Weekly Email Process
# Class responsible for overall program management
#
from datetime import datetime, timedelta
import time
import os
from multiprocessing.dummy import Pool as ThreadPool
from multiprocessing_logging import install_mp_handler
import logging

from jira_manager import JiraManager
from sftp_manager import sFTPManager
from email_manager import EmailManager
from excel_manager import ExcelManager


today_date = (datetime.now() - timedelta(hours=7)).strftime('%Y%m%d')


class DataEnablementEmailManager(object):
    def __init__(self, config_params):
        self.email_file_name = config_params['email_file_name']
        self.running_mode = config_params['running_mode']
        self.sftp_server = config_params['sftp_server']
        self.jira_url = config_params['jira_url']
        self.jira_token = config_params['jira_token']
        self.jira_pars = JiraManager(self.jira_url, self.jira_token, self.email_file_name)
        self.jira_status_parent = config_params['jql_status_parent']
        self.jira_status_child_sftp = config_params['jql_status_child_sftp']
        self.jira_status_child_email = config_params['jql_status_child_email']
        self.jira_issuetype = config_params['jql_issuetype']
        self.jira_label = config_params['jql_label']
        self.jira_text = config_params['jql_text']
        self.ssh_key = config_params['ssh_key']
        self.sftp_url = config_params['sftp_url']
        self.sftp_user = config_params['sftp_user']
        self.sftp_path_to_keyfile = config_params['sftp_path_to_keyfile']
        self.key_file = self.sftp_path_to_keyfile + "ssh_key_file"
        self.sftp_folder_path = config_params['sftp_folder_path']
        self.sftp_zip_file_path = config_params['sftp_zip_file_path']
        self.excel_path = config_params['excel_path']
        self.email_subject = config_params['email_subject']
        self.email_to = config_params['email_to']
        self.email_from = config_params['email_from']
        self.email_cc = config_params['email_cc']
        self.parent_tickets = []
        self.good_parent_tickets = []
        self.run_sftp = ['1', '3']
        self.run_email = ['2', '3']
        self.sftper = None
        self.ftp_files_attr = []
        self.logger = logging.getLogger(__name__)

    # Manages the process for finding tickets, mines excel data file for license information then launches the
    # subprocess routine to run tickets concurrently
    #
    def process_manager(self):
        # pulls desired tickets running jql
        self.parent_tickets = self.jira_pars.find_parent_tickets(self.jira_issuetype, self.jira_status_parent,
                                                                 self.jira_text)
        self.logger.info("{} ticket(s) were found.".format(len(self.parent_tickets)))
        self.logger.info(str([ticket.key for ticket in self.parent_tickets]) + "\n")

        # verifies that parent tickets were found that match the search criteria and logs count and a list of all
        # tickets then pulls issue information
        if self.parent_tickets:
            for parent_ticket in self.parent_tickets:
                self.logger.info("\n\t\t\t\t\t\t\t  => Parent Ticket Number: {}".format(parent_ticket))

                # fetches the relevant parent ticket level information for email population
                parent_ticket.customer_name = self.jira_pars.parent_information_pull(parent_ticket)
                self.logger.info("\t  => Account/Customer name: {}".format(parent_ticket.customer_name))

                # retrieves parent level account information from ZFS1 located excel file for email population
                try:
                    account_data = self.excel_data_fetch(parent_ticket)
                    self.logger.info("\t  => Account/Customer data: {}".format(account_data))
                except Exception as e:
                    self.logger.error("Excel data fetch failed => {} - Moving on to next parent ticket".format(e))
                else:
                    # create the iterable list for concurrent processing
                    self.good_parent_tickets.append([parent_ticket, parent_ticket.customer_name, account_data])
            self.logger.info("\n")

            # run the ftp automation
            if self.running_mode in self.run_sftp:
                # create a ssh private-key file on server and copy the key into it, if no file creation - exit
                try:
                    self.ssh_key_file_create()
                except Exception as e:
                    self.logger.error(
                        "There was a problem creating the key file: {} on the server: {}".format(self.key_file, e))
                    raise SystemExit
                else:
                    # create the sftp object instance
                    self.sftper = sFTPManager(self.sftp_url, self.sftp_user, self.key_file,
                                              self.sftp_folder_path)

                    # open a connection to the ftp server and switch to the assigned directory, if no connection - exit
                    try:
                        self.sftper.open_connection()
                        # remove the private-key file from the server
                        self.ssh_key_file_remove()
                    except Exception as e:
                        self.logger.error(
                            "There was a problem connecting to the sFTP server: {} - {}".format(e, self.sftp_url))
                        raise SystemExit
                    else:
                        for ticket in self.good_parent_tickets:
                            self.ftp_manager(ticket)
                        self.logger.info("\n")
                        self.sftper.close_connection()
                        self.logger.info("\n")
            else:
                self.logger.info("\n")
                self.logger.info("\t***This run was specified to omit ftp posting.***\n")

            # pause between processes to ensure Jira ticket updating
            time.sleep(10)

            # run the email automation
            if self.running_mode in self.run_email:
                # launch the email concurrency manager
                self.concurrency_manager('email', self.mail_manager)
            else:
                self.logger.info("\n")
                self.logger.info("\t***This run was specified to omit email sending.***\n")

        else:
            self.logger.warning("There were no tickets found with the required criteria to report on.")

        self.jira_pars.kill_session()

    # Finds the associated child ticket, collects date range via Jira, creates zip file name and location information,
    # calls function for ftp zip file transfer, calls function for email creation and delivery
    #
    def concurrency_manager(self, function_type, function_call):
        # runs the concurrency for either ftp or email
        self.logger.info("=> Beginning the ticket level - {} concurrent processing.".format(function_type))

        # activate concurrency logging handler
        install_mp_handler(logger=self.logger)
        # set the logging level of urllib3 to "ERROR" to filter out 'warning level' logging message deluge
        logging.getLogger("urllib3").setLevel(logging.ERROR)

        # launches a thread for each of the found tickets
        with ThreadPool(processes=len(self.good_parent_tickets)) as ticket_pool:
            try:
                ticket_pool.map(function_call, self.good_parent_tickets)
                ticket_pool.close()
                ticket_pool.join()
            except Exception as e:
                self.logger.error = ("Ticket Level - {} Concurrency run failed => {}".format(function_type, e))
            else:
                self.logger.info("=> Finished the ticket level - {} concurrent processing.\n".format(function_type))

    # Finds the associated child ticket, collects date range via Jira, creates zip file name and location information,
    # calls function for ftp zip file transfer
    #
    def ftp_manager(self, parent_ticket):
        # pulls desired sub-tasks running jql for ftp posting
        child_ticket_sftp = self.child_ticket_pull(parent_ticket[0], self.jira_status_child_sftp)

        # post zip files to customer ftp site
        if child_ticket_sftp is not None:
            self.logger.info("\n")
            self.logger.info("=> Parent Ticket: {}, Child Ticket: {}".format(parent_ticket[0].key,
                                                                             child_ticket_sftp.key))

            zip_file_zfs_path, zip_file_name = self.zip_file_info(parent_ticket[0], child_ticket_sftp)

            try:
                ftp_file = self.file_sftp(zip_file_zfs_path, zip_file_name)
            except Exception as e:
                self.logger.error("There was a problem with the ftp execution -> {}".format(e))
            else:
                if ftp_file is not None:
                    self.ticket_modifier_sftp(child_ticket_sftp, ftp_file, zip_file_name)
                else:
                    self.logger.error("The presence of the zip file on the ftp site could not be confirmed")
        else:
            self.logger.info("\n")
            self.logger.warning("\t  => Parent Ticket: {}, No child ticket found with the required criteria to process."
                                .format(parent_ticket[0].key))

    # Finds the associated child ticket, collects date range via Jira, creates zip file name and location information,
    # calls function for email creation and delivery
    #
    def mail_manager(self, parent_ticket):
        # Pulls desired sub-tasks running jql for email delivery
        child_ticket_email = self.child_ticket_pull(parent_ticket[0], self.jira_status_child_email)

        # send email to customer about zip file delivery
        if child_ticket_email is not None:
            self.logger.info("=> Parent Ticket: {}, Child Ticket: {}".format(parent_ticket[0].key,
                                                                             child_ticket_email.key))

            child_ticket_email.date_range = self.jira_pars.child_information_pull(child_ticket_email)

            try:
                email_file = self.emailer(parent_ticket[1], child_ticket_email.date_range, parent_ticket[2])
            except Exception as e:
                self.logger.error("There was a problem sending the email -> {}".format(e))
            else:
                self.ticket_modifier_email(child_ticket_email, email_file)
        else:
            self.logger.warning("There was no child ticket found with the required criteria to process.")

    # Creates the Excel Manager instance and fetches the Parent level account data
    #
    def excel_data_fetch(self, ticket):
        excel_data = ExcelManager()
        customer_identifier = excel_data.excel_search(ticket, self.excel_path)
        account_data = excel_data.excel_read(customer_identifier)
        return account_data

    # Finds and returns the sub-task ticket associated with the parent ticket
    #
    def child_ticket_pull(self, parent_ticket, jira_status_child):
        # pulls desired sub-task running jql
        try:
            child_ticket = self.jira_pars.find_child_tickets(parent_ticket, jira_status_child, self.jira_label)
        except Exception as e:
            self.logger.error("There was a problem fetching the child tickets. => {}".format(e))
            return None
        else:
            return child_ticket

    # Pulls the date range from ticket, adds this to the parent ticket and child ticket keys to create zip file path
    # and name
    #
    def zip_file_info(self, parent_ticket, child_ticket):
        # fetches the relevant child ticket level information for sftp posting, create zip file path and name
        try:
            child_ticket.date_range = self.jira_pars.child_information_pull(child_ticket)
        except Exception as e:
            self.logger.error("There was a problem fetching the ticket data range. => {}".format(e))
            return None
        else:
            zip_file_zfs_path = "{}{}/{}/".format(self.sftp_zip_file_path, parent_ticket.key, child_ticket.key)
            zip_file_name = "{}_{}.zip".format(parent_ticket.customer_name, child_ticket.date_range)
            return zip_file_zfs_path, zip_file_name

    # Creates a temporary ssh private-key file on EC2 instance and copies in the key value from the vault
    #
    def ssh_key_file_create(self):
        with open(self.key_file, 'w') as file:
            file.write(self.ssh_key)

    # Removes the temporary ssh private-key file from the EC2 instance
    #
    def ssh_key_file_remove(self):
        os.remove(self.key_file)

    # Creates a sFTP Manager instance, calls the sftp_put module which uploads zip file to client ftp server, retrieves
    # attributes from ftp site and creates file for jira ticket posting, validating file delivery
    #
    def file_sftp(self, child_ticket_zfs_path, zip_file_name):
        # copy zip file from zfs/Technology to designated client ftp site and directory
        try:
            self.sftper.sftp_put(child_ticket_zfs_path, zip_file_name)
        except Exception as e:
            self.logger.error("There was a problem uploading the file: {} to the ftp site: {}".format(zip_file_name, e))
            return None
        else:
            self.logger.info("The zip file {} has been posted on the {} site".format(zip_file_name, self.sftp_url))

        # slight pause to ensure ftp directory has been updated with the file posting, then check for files on server
        time.sleep(5)
        self.ftp_files_attr = self.sftper.get_attributes()

        # creates and returns a stringIO file object of that file for Jira attachment
        try:
            attachment_file = self.sftper.create_stringio(zip_file_name, self.ftp_files_attr)
        except Exception as e:
            self.logger.error("There was a problem creating the file attachment for: {} - {}".format(zip_file_name, e))
            return None
        else:
            #time.sleep(1)
            #print("{}".format(attachment_file.getvalue()))
            #time.sleep(1)
            return attachment_file

    # Creates the Email Manager instance, launches the weekly emailer module
    #
    def emailer(self, customer_name, date_range, account_data):
        weekly_email = EmailManager(date_range, customer_name, account_data, self.email_subject,
                                    self.email_to, self.email_from, self.email_cc, self.sftp_server)
        email_file = weekly_email.weekly_emailer()
        self.logger.info("The email for this ticket has been sent.")
        return email_file

    # Modifies Jira ticket by attaching ftp text file, adding comment and progressing status of ticket to 'Complete'
    #
    def ticket_modifier_sftp(self, ticket, ftp_file, zip_file_name):
        self.jira_pars.add_ftp_attachment(ticket, ftp_file)
        self.logger.info("A copy of the ftp posting time-stamp has been attached as a text file to "
                         "Jira Ticket: {}".format(ticket.key))
        self.jira_pars.add_ftp_posting_comment(ticket, zip_file_name)
        self.logger.info("The zip file/ftp posting alert has been added as a comment to "
                         "Jira Ticket: {}".format(ticket.key))
        self.jira_pars.update_duedate_field(ticket)
        self.jira_pars.progress_ticket(ticket)

    # Modifies Jira ticket by attaching email-text file, adding comment and changing the 'labels' field
    #
    def ticket_modifier_email(self, ticket, email_file):
        self.jira_pars.add_email_attachment(ticket, email_file)
        self.logger.info("A copy of the email has been attached as a text file to Jira Ticket: {}".format(ticket.key))
        self.jira_pars.add_rr_alert_comment(ticket)
        self.logger.info(
            "A Revenue Recognition alert has been added as a comment to Jira Ticket: {}".format(ticket.key))
        self.jira_pars.update_labels_field(ticket)

    # Checks the log directory for all files and removes those after a specified number of days
    #
    def purge_files(self, purge_days, purge_dir):
        try:
            self.logger.info("\n\t\tRemove {} days old files from the {} directory".format(purge_days, purge_dir))
            now = time.time()
            for file_purge in os.listdir(purge_dir):
                f_obs_path = os.path.join(purge_dir, file_purge)
                if os.stat(f_obs_path).st_mtime < now - int(purge_days) * 86400:
                    time_stamp = time.strptime(time.strftime('%Y-%m-%d %H:%M:%S',
                                               time.localtime(os.stat(f_obs_path).st_mtime)), "%Y-%m-%d %H:%M:%S")
                    self.logger.info("Removing File [{}] with timestamp [{}]".format(f_obs_path, time_stamp))
                    os.remove(f_obs_path)

        except Exception as e:
            self.logger.warning("{}".format(e))
