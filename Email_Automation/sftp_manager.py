# sftp_manager module
# Module holds the class => sFTPManager - manages sFTP interface
# Class responsible for all sFTP related interactions including connecting to server and posting files
#
import pysftp
import time
from io import StringIO
import logging


class sFTPManager(object):
    def __init__(self, sftp_url, sftp_user, path_to_keyfile, sftp_folder_path):
        self.sftp_url = sftp_url
        self.sftp_user = sftp_user
        self.path_to_keyfile = path_to_keyfile
        self.sftp_folder_path = sftp_folder_path
        self.sftp = None
        self.logger = logging.getLogger(__name__)

    # Opens a sFTP connection to server and changes to correct directory
    #
    def open_connection(self):
        self.sftp = pysftp.Connection(self.sftp_url, username=self.sftp_user, private_key=self.path_to_keyfile)
        self.sftp.cwd(self.sftp_folder_path)
        return self.sftp

    # Copies the zip file from zfs/Technology location to ftp server
    #
    def sftp_put(self, child_ticket_zfs_path, zip_file_name):
        self.sftp.put("{}{}".format(child_ticket_zfs_path, zip_file_name),
                      "{}{}".format(self.sftp_folder_path, zip_file_name))

    # Retrieves the attributes for the zip file after it has been copied to ftp server, includes required time stamp
    #
    def get_attributes(self):
        return self.sftp.listdir_attr()

    # Retrieves a directory list from ftp server
    #
    def dir_list(self):
        return self.sftp.listdir()

    # Creates a stringIO file object for the Jira ticket as a verification of zip file placement on ftp server
    #
    def create_stringio(self, file_name, attributes):
        attachment_file = StringIO()
        msg = ""
        for attribute in attributes:
            if attribute.filename == file_name:
                self.logger.info("ftp file attributes: {}".format(attribute))
                msg += 'File Name: {}'.format(attribute.filename)
                msg += '\n\tLocation on ftp Server ->    {}:{}'.format(self.sftp_url.ljust(20), self.sftp.pwd.rjust(20))
                msg += '\n\tLast Access Time             {}:{}'.format("".ljust(20), time.ctime(attribute.st_atime).rjust(20))
                msg += '\n\tCreated or Last Modified Time{}:{}'.format("".ljust(20), time.ctime(attribute.st_mtime).rjust(20))
                msg += '\n\tFile Size                    {}:{:,} bytes'.format("".ljust(20), attribute.st_size)
                attachment_file.write(msg)
                return attachment_file
            else:
                pass

        self.logger.info("The ftp file: {}, was not found on the server".format(file_name))
        return attachment_file

    # Closes the sFTP connection
    #
    def close_connection(self):
        self.sftp.close()
