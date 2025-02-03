# weekly_emailer module
# Module holds the class => WeeklyEmailManager - manages the email creation and the smtp interface
# Class responsible for all email related management
#
from smtplib import SMTP
from email.message import EmailMessage
from datetime import datetime, timedelta
from io import StringIO
import logging


class EmailManager(object):
	def __init__(self, date_range, customer_name, account_data, subject, to_address, from_address, cc, sftp_server):
		date = (datetime.now() - timedelta(hours=7)).strftime("%Y%m%d")
		self.logger = logging.getLogger(__name__)
		self.message = ""
		self.market_id = account_data['market_id']
		self.beacon_id = account_data['beacon_id']
		self.data_contract_id = account_data['data_contract_id']
		self.file_name = "{}_{}.zip".format(customer_name, date_range)
		self.msg = ""
		self.attachment = StringIO()
		self.subj = subject
		self.to_address = to_address
		self.from_address = from_address
		self.cc = cc
		self.text = \
		"Amobee Support,\n\n \
		Oracle has delivered offline purchase data on {date}. Please find details below:\n\n \
		Market ID: {mid}\n\n \
		Beacon ID: {bid}\n\n \
		Data Contract ID: {dcid}\n\n \
		sFTP Directory: {server}\n\n \
		Filename: {fn}\n\n \
		Data Date Range: {dr}\n\n \
		Thanks,\n \
		Oracle Team".format(date=date, mid=self.market_id, bid=self.beacon_id, dcid=self.data_contract_id,
							server=sftp_server, fn=self.file_name, dr=date_range)

	# Create the email in a text format then send via smtp, finally save the email as a StringIO file and return
	#
	def weekly_emailer(self):
		try:
			# Simple Text Email
			self.msg = EmailMessage()
			self.msg['Subject'] = self.subj
			self.msg['From'] = self.from_address
			self.msg['To'] = self.to_address
			self.msg['Cc'] = self.cc

			# Message Text
			self.msg.set_content(self.text)

			# Send Email
			with SMTP('mailhost.valkyrie.net') as smtp:
				smtp.send_message(self.msg)

		except Exception as e:
			self.logger.error("Email failed => {}".format(e))

		else:
			# Convert email to StringIO and return
			msg_txt = bytes(self.msg).decode('utf-8')
			self.attachment.write(msg_txt)
			self.attachment.seek(0)
		return self.attachment
