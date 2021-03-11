# coding=utf-8
#
# Read Bloomberg Repo XML file, add headers, then upload to Geneva.
# 
from aim_xml.add_header import addRepoHeaders, isRepoMaster, isRepoTrade \
							, isRepoRerate, isRepoDummyRerate, isRepoResize
from aim_xml.utility import getDataDirectory, getMailSender, getMailServer \
							, getMailTimeout, getNotificationMailRecipients \
							, getSftpTimeout, getWinScpPath, getCurrentDir \
							, getSftpUser, getSftpPassword, getSftpServer \
							, getDatetimeAsString
from aim_xml.constants import Constants
from repo_data.repo_datastore import saveRepoMasterFileToDB, saveRepoTradeFileToDB \
							, saveRepoRerateFileToDB
from steven_utils.file import getFiles, getFilenameWithoutPath
from steven_utils.mail import sendMail
from toolz.functoolz import compose
from os.path import join
from subprocess import run
import shutil
import logging
logger = logging.getLogger(__name__)



"""
	[String] file type => [List] files of that type
"""
getFilesByType = lambda fileType: \
compose(
	list
  , lambda files: \
  		filter(isRepoMaster, files) if fileType == 'master' else \
  		filter(isRepoTrade, files) if fileType == 'trade' else \
  		filter(isRepoRerate, files) if fileType == 'rerate' else \
  		filter(isRepoDummyRerate, files) if fileType == 'dummy_rerate' else \
  		filter(isRepoResize, files) if fileType == 'resize' else []
  , lambda directory: getFiles(directory, True)
  , getDataDirectory
)()



def handleRepoFiles(fileType):
	"""
	[String] file type
		=> ([Int] status, [String] message, [List] files)

	This function does not throw any exceptions.

	1. get repo files based on its type (master, trade, rerate
		dummy_rerate, resize)
	2. Take action on the file types: add header and upload, or
		give warning (if it's resize)
	3. send notification email.
	4. move input files to another directory
	"""
	logger.debug('handleRepoFiles(): {0}'.format(fileType))

	if not fileType in ('resize', 'master', 'trade', 'rerate', 'dummy_rerate'):
		return (Constants.STATUS_ERROR, 'invalid file type {0}'.format(fileType), [])

	status01, message01 = saveToDatastore(fileType)
	status02, message02, files = addHeaderAndUpload(fileType)
	message = message01 + '\n\n' + message02

	return \
	(Constants.STATUS_ERROR, message, files) if Constants.STATUS_ERROR in (status01, status02) else \
	(Constants.STATUS_WARNING, message, files) if Constants.STATUS_WARNING in (status01, status02) else \
	(Constants.STATUS_SUCCESS, message, files)



def saveToDatastore(fileType):
	"""
	[String] file type
		=> ([Int] status, [String] message)

	Assume fileType is one of 5 types: resize, master, trade, rerate,
	and dummy_rerate
	"""
	logger.debug('saveToDatastore(): {0}'.format(fileType))
	if fileType in ('resize', 'dummy_rerate'):
		return (Constants.STATUS_SUCCESS, 'Nothing saved to db for {0}'.format(fileType))

	handler = saveRepoMasterFileToDB if fileType == 'master' else \
				saveRepoTradeFileToDB if fileType == 'trade' else \
				saveRepoRerateFileToDB

	try:
		total = 0
		for fn in getFilesByType(fileType):
			total = total + handler(fn)

		return (Constants.STATUS_SUCCESS, '{0} records saved to datestore'.format(total))

	except:
		logger.exception('saveToDatastore()')
		return (Constants.STATUS_ERROR, 'Saving to datestore failed')



def addHeaderAndUpload(fileType):
	"""
	[String] file type
		=> ([Int] status, [String] message, [List] files)

	Assume fileType is one of 5 types: resize, master, trade, rerate,
	and dummy_rerate
	"""
	logger.debug('addHeaderAndUpload(): {0}'.format(fileType))

	files = []
	try:
		files = getFilesByType(fileType)
		if fileType == 'resize':
			return (Constants.STATUS_WARNING, '\n'.join(files), files)

		filesWithHeader = list(map(addRepoHeaders, files))
		upload('A2GTrade', filesWithHeader)
		return ( Constants.STATUS_SUCCESS if filesWithHeader != [] \
					else Constants.STATUS_WARNING
			   , '\n'.join(filesWithHeader), files + filesWithHeader)

	except:
		logger.exception('addHeaderAndUpload()')
		return (Constants.STATUS_ERROR, '\n'.join(files), files)



def sendNotificationEmail(fileType, status, message):
	"""
	[String] fund name, [Int] status, [String] message

	send email to notify the status. 
	"""
	logger.debug('sendNotificationEmail():')

	getSubject = lambda fileType, status: \
		'Repo ' + fileType + ' upload succesful' \
		if status == Constants.STATUS_SUCCESS else \
		'Warning: Repo ' + fileType \
		if  status == Constants.STATUS_WARNING else \
		'Repo ' + fileType + ' upload failed'


	sendMail( message
			, getSubject(fileType, status)
			, getMailSender()
			, getNotificationMailRecipients()
			, getMailServer()
			, getMailTimeout())



def moveFiles(outputDir, files):
	"""
	[String] output directory,
	[List] files (with full path)

	Side effect: move files to the output directory, after moving
	the files will be renamed with date time string in their name.
	"""
	""" 
		[String] fn => [String] fn with date time 

		Assume: fn is without path and have only one '.' in it, like
		xxxxx.xml
	"""
	rename = compose(
		lambda L: L[0] + '_' + getDatetimeAsString() + '.' + L[1]
	  , lambda fn: fn.split('.')
	)

	for fn in files:
		logger.debug('moveFiles(): {0}'.format(fn))
		shutil.move( fn
				   , join( outputDir
				   		 , rename(getFilenameWithoutPath(fn))))



def upload(remoteDir, files):
	"""
	[String] remote directory, [List] files 

	side effect: upload the files to Geneva SFTP server.

	Note: this function calls Windows Application WinScp to connect
	to the SFTP server. Before running this problem, use the WinScp
	to connect to the server at least once, to make sure the server
	key is acceptable to the caller.
	"""
	logger.debug('upload(): {0}'.format(','.join(files)))

	def createWinScpScript(files):
		"""
		[List] files => [String] WinScp script file

		Side effect: create a script to be used by WinScp program
		"""
		scriptFile = join( getCurrentDir(), 'winscp_scripts'
						 , 'run-sftp_{0}.txt'.format(getDatetimeAsString()))
		
		with open(scriptFile, 'w') as f:
			f.write('open sftp://{0}:{1}@{2}\n'.format(getSftpUser(), \
					getSftpPassword(), getSftpServer()))

			f.write('cd {0}\n'.format(remoteDir))
			for file in files:
				f.write('put {0}\n'.format(file))

			f.write('exit')

		return scriptFile
	# end of createWinScpScript()

	if len(files) > 0:
		run( [ getWinScpPath()
			 , '/script={0}'.format(createWinScpScript(files))
			 , '/log={0}'.format(join(getCurrentDir(), 'logs', 'winscp_log.log'))
			 ]
		   , timeout=getSftpTimeout()
		   , check=True)




if __name__ == "__main__":
	import logging.config
	logging.config.fileConfig('logging.config', disable_existing_loggers=False)
	
	import argparse
	parser = argparse.ArgumentParser(description='Handle repo xml files')
	parser.add_argument( 'fileType', metavar='fileType', type=str
					   , help="repo XML file type")

	"""
	There are 5 file types to handle: master, trade, rerate, 
	dummy_rerate, and resize
	
	To run the program, do:

		$ python repo_upload.py <file type>

	"""
	fileType = parser.parse_args().fileType
	status, message, files = handleRepoFiles(fileType)
	sendNotificationEmail(fileType, status, message)
	moveFiles(join(getDataDirectory(), 'SENT'), files)
