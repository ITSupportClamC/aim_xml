# coding=utf-8
#
# Read Bloomberg repo master, trade, and rerate files, extract
# information and save them into database.
# 
from repo_data.data import initializeDatastore
from repo_data.repo_datastore import saveRepoMasterFileToDB \
									, saveRepoTradeFileToDB \
									, saveRepoRerateFileToDB
from aim_xml.constants import Constants
from aim_xml.repo_upload import getFilesByType
from aim_xml.utility import sendNotificationEmail
from toolz.functoolz import compose
from itertools import chain
from functools import partial
import logging
logger = logging.getLogger(__name__)



def saveToDatastore(fileType, files):
	"""
	[String] fileType, [List] files
		=> ([Int] status, [Int] no. of records saved)

	Assume: (1) fileType is master, trade, or rerate.
			(2) files is not empty;

	This function does not throw any exceptions.
	"""
	logger.debug('saveToDatastore(): {0}'.format(fileType))

	handler = saveRepoMasterFileToDB if fileType == 'master' else \
				saveRepoTradeFileToDB if fileType == 'trade' else \
				saveRepoRerateFileToDB

	try:
		total = sum(map(handler, files))

		return (Constants.STATUS_SUCCESS, total) if total > 0 else \
		(Constants.STATUS_ERROR, total)

	except:
		logger.exception('saveToDatastore()')
		return (Constants.STATUS_ERROR, total)



def handleRepoFiles(*fileTypes):
	"""
	[List] file types
		=> ([String] subject, [String] message)

	This function does not throw any exceptions.
	"""
	logger.debug('handleRepoFiles()')

	def getSaveResult(fileType):
		"""
		[String] file type => ( [Int] status
							  , [Int] no. of records saved to db
							  , [List] files processed)

		This function does not throw exceptions
		"""
		logger.debug('getSaveResult(): {0}'.format(fileType))
		try:
			files = getFilesByType(fileType)
		except:
			logger.exception('getSaveResult()')
			return (Constants.STATUS_ERROR, 0, [])

		if files == []:
			logger.debug('getSaveResult(): no input files found')
			return (Constants.STATUS_NO_INPUT, 0, [])

		status, numRecords = saveToDatastore(fileType, files)
		return (status, numRecords, files)


	""" 
		[List] ([Int] status) => [String] subject 
		Assume: len(status list) > 0
	"""
	getSubject = lambda statusList: \
		'AIM repo database save failed' \
		if any(map(lambda el: el == Constants.STATUS_ERROR, statusList)) else \
		'AIM repo database no input files found' \
		if all(map(lambda el: el == Constants.STATUS_NO_INPUT, statusList)) else \
		'AIM repo database save successful'


	getMessage = lambda numRecordsList, filesList: \
		'{0} records saved to database'.format(sum(numRecordsList)) \
		+ '\n\nFiles processed:\n' \
		+ '\n'.join(chain.from_iterable(filesList))


	return \
	compose(
		lambda t: (getSubject(t[0]), getMessage(t[1], t[2]))
	  , lambda L: ( list(map(lambda t: t[0], L))
	  			  , map(lambda t: t[1], L)
	  			  , map(lambda t: t[2], L)
	  			  )
	  , list
	  , partial(map, getSaveResult)
	)(fileTypes)




if __name__ == "__main__":
	import logging.config
	logging.config.fileConfig('logging.config', disable_existing_loggers=False)
	
	logger.debug('main(): start')
	initializeDatastore('production')
	subject, message = handleRepoFiles('master', 'trade', 'rerate')
	sendNotificationEmail(subject, message)
