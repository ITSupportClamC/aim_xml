# coding=utf-8
#
# Read Bloomberg Repo XML file, add headers, then upload to Geneva.
# 
from aim_xml.add_header import addRepoHeaders, isRepoMaster, isRepoTrade \
							, isRepoRerate, isRepoResize
from aim_xml.utility import getDataDirectory, getMailSender, getMailServer \
							, getMailTimeout, getNotificationMailRecipients \
							, getSftpTimeout, getWinScpPath, getCurrentDir \
							, getSftpUser, getSftpPassword, getSftpServer \
							, getDatetimeAsString
from aim_xml.constants import Constants
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
  		filter(lambda fn: isRepoTrade(fn) or isRepoRerate(fn), files) \
  		if fileType == 'transaction' else \
  		filter(isRepoResize, files) if fileType == 'resize' else []
  , lambda directory: getFiles(directory, True)
  , getDataDirectory
)()



def handleRepoFiles(fileType):
	"""
	[String] file type
		=> [Tuple] ([Int] status, [String] message, [List] files)

	This function does not throw any exceptions.

	1. get repo files based on its type (master, transaction, resize)

	3. add header
	4. send notification email.
	5. move input files to another directory
	"""
	logger.debug('handleRepoFiles(): {0}'.format(fileType))

	files = []
	try:
		files = getFilesByType(fileType)
		if fileType == 'resize':
			return (Constants.STATUS_WARNING, '\n'.join(files), files)

		if not fileType in ('master', 'transaction'):
			return (Constants.STATUS_ERROR, 'invalid file type {0}'.format(fileType), files)

		filesWithHeader = list(map(addRepoHeaders, files))
		upload(filesWithHeader)
		return ( Constants.STATUS_SUCCESS if filesWithHeader != [] \
					else Constants.STATUS_WARNING
			   , '\n'.join(filesWithHeader), files + filesWithHeader)

	except:
		logger.exception('handleRepoFiles()')
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

	Side effect: move files to the output directory
	"""
	for fn in files:
		logger.debug('moveFiles(): {0}'.format(fn))
		shutil.move(fn, join(outputDir, getFilenameWithoutPath(fn)))



def upload(files):
	"""
	[List] files 

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

			f.write('cd A2GTrade\n')
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
	There are 3 file types to handle: master, transaction, and resize
	
	To run the program, do:

		$ python repo_upload.py <file type>

	"""
	fileType = parser.parse_args().fileType
	status, message, files = handleRepoFiles(fileType)
	sendNotificationEmail(fileType, status, message)
	moveFiles(join(getDataDirectory(), 'SENT'), files)
