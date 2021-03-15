# coding=utf-8
#
# Functions related to Bloomberg AIM XML trade file.
# 
from os.path import dirname, abspath, join
from datetime import datetime
from functools import lru_cache
import logging, configparser
logger = logging.getLogger(__name__)



""" [String] date time converted to String """
getDatetimeAsString = lambda: \
	datetime.strftime(datetime.now(), '%Y%m%d%H%M%S')



getCurrentDir = lambda: dirname(abspath(__file__))



@lru_cache(maxsize=3)
def loadConfigFile(file):
	"""
	Read the config file, convert it to a config object.
	"""
	cfg = configparser.ConfigParser()
	cfg.read(join(getCurrentDir(), file))
	return cfg



getDataDirectory = lambda : \
	loadConfigFile('aim_xml.config')['data']['directory']



getMailSender = lambda : \
	loadConfigFile('aim_xml.config')['email']['sender']



getMailServer = lambda : \
	loadConfigFile('aim_xml.config')['email']['server']



getMailTimeout = lambda : \
	float(loadConfigFile('aim_xml.config')['email']['timeout'])



getNotificationMailRecipients = lambda : \
	loadConfigFile('aim_xml.config')['email']['notificationMailRecipients']



getSftpTimeout = lambda : \
	float(loadConfigFile('aim_xml.config')['sftp']['timeout'])



getWinScpPath = lambda : \
	loadConfigFile('aim_xml.config')['sftp']['winscpPath']



getSftpUser = lambda : \
	loadConfigFile('aim_xml.config')['sftp']['username']



getSftpPassword = lambda : \
	loadConfigFile('aim_xml.config')['sftp']['password']



getSftpServer = lambda : \
	loadConfigFile('aim_xml.config')['sftp']['server']



def sendNotificationEmail(subject, message):
	"""
	[String] subject, [String] message

	send email to notify the status. 
	"""
	logger.debug('sendNotificationEmail():')
	sendMail( message
			, subject
			, getMailSender()
			, getNotificationMailRecipients()
			, getMailServer()
			, getMailTimeout())