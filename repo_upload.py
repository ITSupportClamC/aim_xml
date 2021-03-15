# coding=utf-8
#
# Read Bloomberg Repo XML file, add headers, then upload to Geneva.
# 
from aim_xml.add_header import addRepoHeaders, isRepoMaster, isRepoTrade \
							, isRepoRerate, isRepoDummyRerate, isRepoResize
from aim_xml.utility import getDataDirectory, sendNotificationEmail \
							, getSftpTimeout, getWinScpPath, getCurrentDir \
							, getSftpUser, getSftpPassword, getSftpServer \
							, getDatetimeAsString
from aim_xml.constants import Constants
from repo_data.repo_datastore import saveRepoMasterFileToDB, saveRepoTradeFileToDB \
							, saveRepoRerateFileToDB
from repo_data.data import initializeDatastore
from steven_utils.file import getFiles, getFilenameWithoutPath
from toolz.functoolz import compose
from functools import partial
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
	"""
	logger.debug('handleRepoFiles(): {0}'.format(fileType))

	if not fileType in ('resize', 'master', 'trade', 'rerate', 'dummy_rerate'):
		return (Constants.STATUS_ERROR, 'invalid file type {0}'.format(fileType), [])

	try:
		files = getFilesByType(fileType)
	except:
		logger.exception('handleRepoFiles()')
		return (Constants.STATUS_ERROR, 'Failed to get {0} files'.format(fileType), [])

	if files == []:
		return (Constants.STATUS_NO_INPUT, 'No {0} files'.format(fileType), [])

	return \
	addHeaderAndUpload(files) if fileType in ('master', 'trade', 'rerate', 'dummy_rerate') \
	else (Constants.STATUS_WARNING, '\n'.join(files), files)



def addHeaderAndUpload(files):
	"""
	[List] files
		=> ([Int] status, [String] message, [List] files)

	Assume files are are one of 4 types: master, trade, rerate, 
	or dummy_rerate

	This function does not throw any exceptions.
	"""
	logger.debug('addHeaderAndUpload()')
	try:
		filesWithHeader = list(map(addRepoHeaders, files))
		upload('A2GTrade', filesWithHeader)

		return ( Constants.STATUS_SUCCESS
			   , '\n'.join(filesWithHeader)
			   , files + filesWithHeader
			   )

	except:
		logger.exception('addHeaderAndUpload()')
		return (Constants.STATUS_ERROR, '\n'.join(files), files)



def sendUploadNotification(fileType, status, message):
	"""
	[String] fund name, [Int] status, [String] message

	send email to notify the status.
	"""
	logger.debug('sendUploadNotification():')

	getSubject = lambda fileType, status: \
		'Repo ' + fileType + ' no files found' \
		if status == Constants.STATUS_NO_INPUT else \
		'Repo ' + fileType + ' upload succesful' \
		if status == Constants.STATUS_SUCCESS else \
		'Warning: Repo ' + fileType \
		if  status == Constants.STATUS_WARNING else \
		'Error: Repo ' + fileType + ' upload failed'

	sendNotificationEmail(getSubject(fileType, status), message)



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
	logger.debug('moveFiles()')

	rename = compose(
		partial(join, outputDir)
	  , lambda L: L[0] + '_' + getDatetimeAsString() + '.' + L[1]
	  , lambda fn: fn.split('.')
	  , getFilenameWithoutPath
	)

	for fn in files:
		logger.debug('move file: {0}'.format(fn))
		try:
			shutil.move(fn, rename(fn))
		except:
			logger.exception('moveFiles()')



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
	initializeDatastore('production')
	status, message, files = handleRepoFiles(fileType)
	sendUploadNotification(fileType, status, message)
	moveFiles(join(getDataDirectory(), 'SENT'), files)