# jira_manager module
# Module holds the class => JiraManager - manages JIRA ticket interface
# Class responsible for all JIRA related interactions including ticket searching, data pull, file attaching, comment
# posting and field updating.
#
from jira import JIRA
import re
from datetime import datetime, timedelta
from operator import attrgetter
import logging


class JiraManager(object):
    def __init__(self, url, jira_token, email_file_name):
        self.parent_tickets = []
        self.child_tickets = None
        self.jira = JIRA(url, basic_auth=jira_token)
        self.date_range = ""
        self.file_name = ""
        self.advert_field_name = ""
        self.advertiser_name = ""
        self.logger = logging.getLogger(__name__)
        self.today_date = (datetime.now() - timedelta(hours=6)).strftime('%Y-%m-%d')
        self.ftp_posting_alert = 'The file has been loaded to the 1-Turn_Data_LicensingFiles directory on the ' \
                                 'ftp2.turn site.'
        self.email_file_name = "{}.txt".format(email_file_name)
        self.alert_name = 'RevenueRecognition'
        self.revenue_recognition_alert = 'the report delivery email has been attached.'

    # Searches Jira for all tickets that match the parent ticket query criteria
    #
    def find_parent_tickets(self, issuetype, status, text):
        # Query to find qualified Jira Tickets, includes matches for text: including 'Turn' but excluding 'Test'
        jql_query = "project IN (CAM) AND issuetype = " + issuetype + " AND status in " + status + " AND summary ~ " \
                    + text + " ORDER BY " + " key "
        self.parent_tickets = self.jira.search_issues(jql_query)
        return self.parent_tickets

    # Retrieves the required data from parent ticket to populate email
    #
    def parent_information_pull(self, ticket):
        ticket = self.jira.issue(ticket.key)
        # Selects the final split value in the 'Summary' field and strips it of beginning and ending whitespace
        self.advert_field_name = ticket.fields.summary.split('-')[-1].strip()
        # Creates a name list split along whitespace and also splits if CamelHump notation exists
        split_name = re.sub('(?!^)([A-Z][a-z]+)', r' \1', self.advert_field_name).split()
        # Remove '_' character from words in list if exists
        split_name = ' '.join(split_name).replace('_', '').split()
        self.advertiser_name = self.normalize_name(split_name)
        return self.advertiser_name

    # Searches Jira for tickets that are sub-tasks of the list of parent tickets and require an email to be sent
    #
    def find_child_tickets(self, ticket, status, label):
        jql_query = "parent in (" + ticket.key + ") AND status = " + status + " AND labels = " + label
        self.child_tickets = self.jira.search_issues(jql_query)
        if self.child_tickets:
            # this ensures you are returning only the latest child ticket, useful for dev purposes
            self.child_tickets.sort(key=attrgetter('key'), reverse=True)
            # return the latest ticket from list
            return self.child_tickets.pop(0)
        else:
            return None

    # Retrieves the required data from child ticket to populate email
    #
    def child_information_pull(self, ticket):
        ticket = self.jira.issue(ticket.key)
        start_date = datetime.strptime(ticket.fields.customfield_10431, "%Y-%m-%d").strftime("%Y-%m-%d")
        end_date = datetime.strptime(ticket.fields.customfield_10418, "%Y-%m-%d").strftime("%Y-%m-%d")
        self.date_range = "{}_{}".format(start_date, end_date)
        return self.date_range

    # Add a text file copy of ftp time stamp as an attachment to ticket
    #
    def add_ftp_attachment(self, ticket, attachment):
        self.jira.add_attachment(issue=ticket.key, attachment=attachment, filename="ftp_time_stamp.txt")
        self.jira.add_attachment(issue=ticket.key, attachment=attachment, filename="ftp_time_stamp.txt.png")

    # Add a text file copy of email as an attachment to ticket
    #
    def add_email_attachment(self, ticket, attachment):
        self.jira.add_attachment(issue=ticket.key, attachment=attachment, filename=self.email_file_name)

    # Add a comment on ticket with zip file posting alert
    #
    def add_ftp_posting_comment(self, ticket, zip_file_name):
        ticket = self.jira.issue(ticket.key)

        reporter = ticket.fields.reporter.key
        message = """{zip_alert}

                     {zip_file_name}.zip""".format(reporter, zip_alert=self.ftp_posting_alert,
                                                   zip_file_name=zip_file_name)
        self.jira.add_comment(issue=ticket, body=message)

    # Add a comment on ticket to alert 'Revenue Recognition' that a copy of email has been attached to ticket
    #
    def add_rr_alert_comment(self, ticket):
        ticket = self.jira.issue(ticket.key)
        reporter = ticket.fields.reporter.key
        message = """[~{attention}] {rr_alert}""".format(reporter, attention=self.alert_name,
                                                         rr_alert=self.revenue_recognition_alert)
        self.jira.add_comment(issue=ticket, body=message)

    # Change the field 'Due' on the child ticket to the current date
    #
    def update_duedate_field(self, ticket):
        ticket.fields.duedate = self.today_date
        ticket.update(fields={'duedate': ticket.fields.duedate})

    # Change the field 'labels' in the child ticket to the value 'Email_Sent' to omit from future search results
    #
    @staticmethod
    def update_labels_field(ticket):
        # first, remove any existing labels
        ticket.update(labels=None)
        # next, create and add a new custom label
        ticket.fields.labels.append(u'Email_Sent')
        ticket.update(fields={'labels': ticket.fields.labels})

    # Transition the ticket status field to 'Complete' -> id ='621' w/o Rev-Rec
    #
    def progress_ticket(self, ticket):
        ticket = self.jira.issue(ticket.key)
        self.jira.transition_issue(ticket, '621')

    # Applies rules to normalize the Advertiser names into Data Enablement accepted file-naming convention
    #
    @staticmethod
    def normalize_name(split_name):
        # Rules for Del Monte, can have either 3 or 4 names
        if split_name[0] == 'Del':
            if len(split_name) > 3:
                d = [split_name[0] + split_name[1], split_name[2] + split_name[3]]
                advertiser_name = "_".join(d)
            else:
                d = [split_name[0] + split_name[1], split_name[2]]
                advertiser_name = "_".join(d)
        # Rules for Cytosport, can have 4 names
        elif split_name[0] == 'Cytosport':
            d = [split_name[0], split_name[1] + split_name[2] + split_name[3]]
            advertiser_name = "_".join(d)
        # Rules for Colgate and Blackbox, can have either 2 or 3 names
        else:
            if len(split_name) > 2:
                d = [split_name[0], split_name[1] + split_name[2]]
                advertiser_name = "_".join(d)
            else:
                advertiser_name = "_".join(split_name)

        return advertiser_name

    # Ends the current JIRA session
    #
    def kill_session(self):
        self.jira.kill_session()
