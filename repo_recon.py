# coding=utf-8
#
# Read repo reconciliation file from Bloomberg AIM, enrich it with
# collateral id and quantity from datastore, then create the new
# reconciliation file.
# 
from aim_xml.utility import getDataDirectory
from aim_xml.repo_upload import upload, moveFiles, sendNotificationEmail
from aim_xml.constants import Constants
from steven_utils.file import getFiles
from toolz.functoolz import compose
from functools import partial
from itertools import chain
from datetime import datetime, timedelta
from os.path import join
import logging
logger = logging.getLogger(__name__)



def getBloombergReconFiles(directory):
	"""
	[String] directory => [Tuple] file1, file2
	"""
	logger.debug('getBloombergReconFiles(): {0}'.format(directory))
	files = compose(
		list
	  , partial(filter, lambda fn: fn.starswith('Repo_PosRecon_'))
	  , getFiles
	)(directory)

	F1 = list(filter(lambda fn: fn.endswith('_1.csv'), files))
	F2 = list(filter(lambda fn: fn.endswith('_2.csv'), files))

	if len(F1) == len(F2) and len(F1) == 1 and \
		getDateFromFilename(F1[0]) == getDateFromFilename(F2[0]):

		return (join(directory, F1[0]), join(directory, F2[0]))

	else:
		logger.error('getBloombergReconFiles(): invalid number of files')
		raise ValueError



def getDateFromFilename(reconFile):
	"""
	[String] file => [String] date (yyyy-mm-dd)

	From Bloomberg recon file, get the T day date.
	"""
	return ''



def loadRepoPosition(file1, file2):
	"""
	[String] Bloomberg recon file 1 (for closed positions)
	[String] Bloomberg recon file 2 (for open positions)
		=> [Iterable] ([Dictionary] repo position)

	load positions from csv
	for file 1, filter those expired on T+1
	"""
	def getPositions(file):
		""" [String] file => [Iterable] ([Dictionary] position) """
		logger.debug('loadRepoPosition(): {0}'.format(file))
		return []


	# [String] yyyymmdd => [String] yyyy-mm-dd (after increasing 1 day)
	getNextDayDate = compose(
		lambda d: datetime.strftime(d, '%Y-%m-%d')
	  , lambda d: d + timedelta(days=1)
	  , lambda s: datetime.strptime(s, '%Y%m%d')
	)


	getClosedPositions = partial(
		filter
	  , lambda p: \
	  		p['CloseDate'] == getNextDayDate(getDateFromFilename(file1))
	)


	getOpenPositions = partial(
		filter
	  , lambda p: p['LoanAmount'] != 0 and \
	  		p['CloseDate'] > getNextDayDate(getDateFromFilename(file1))
	)


	logger.debug('loadRepoPosition(): {0}, {1}'.format(file1, file2))
	closedPositions = filter( )
	openPositions = compose(

	)
	
	return chain(closedPositions, openPositions)



def enrichPosition(repoData, position):
	"""
	[Dictionary] repoData, [Dictionary] position 
		=> [Dictionary] enriched position

	Add collateral id and quantity to a Bloomberg repo position
	"""
	return {}



def getRepoData():
	"""
	Returns:
	[Dictionary] ([String] repo name -> 
				  [Tuple] ([String] collateral Id, [Quantity] quantity)
				 )
	Where there are multiple collaterals under one repo name, the
	collateral id will be a comma separated string of all collateral
	ids, the quantity will be the sum of of all collateral quantities.
	"""
	return {}



def createRepoReconFile(directory, date, positions):
	"""
	[Iterable] repo positions after enrichment
		=> [String] file

	Create a csv file from the repo positions in the directory.
	"""
	return ''



def doUpload(file):
	"""
	[String] file => [String] file

	Side effect: upload the recon file to SFTP
	"""
	upload('A2GPosition', [file])
	return file




if __name__ == "__main__":
	import logging.config
	logging.config.fileConfig('logging.config', disable_existing_loggers=False)
	
	logger.debug('main(): start')
	try:
		bloombergReconFiles = getBloombergReconFiles(getDataDirectory())
		outputFile = compose(
			doUpload
		  , partial( createRepoReconFile, getDataDirectory()
		  		   , getDateFromFilename(bloombergReconFiles[0]))
		  , partial(map, partial(enrichPosition, getRepoData()))
		  , lambda t: loadRepoPosition(t[0], t[1])
		)(bloombergReconFiles)

	except:
		logger.exception('main()')
		sendNotificationEmail('AIM reconciliation', Constants.STATUS_ERROR, '')
		moveFiles( join(getDataDirectory(), 'Position_SENT')
				 , bloombergReconFiles)

	else:
		logger.debug('main(): success')
		sendNotificationEmail( 'AIM reconciliation', Constants.STATUS_SUCCESS
							 , outputFile)
		moveFiles( join(getDataDirectory(), 'Position_SENT')
				 , bloombergReconFiles + [outputFile])
