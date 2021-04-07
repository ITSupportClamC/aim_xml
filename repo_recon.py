# coding=utf-8
#
# Read repo reconciliation file from Bloomberg AIM, enrich it with
# collateral id and quantity from datastore, then create the new
# reconciliation file.
# 
from aim_xml.utility import getDataDirectory, sendNotificationEmail
from aim_xml.repo_upload import upload, moveFiles
from aim_xml.constants import Constants
from repo_data.data import initializeDatastore, getRepo
from steven_utils.file import getFiles, getFilenameWithoutPath
from steven_utils.utility import mergeDict, dictToValues, writeCsv
from toolz.functoolz import compose
from toolz.itertoolz import groupby as groupbyToolz
from toolz.dicttoolz import valmap
from functools import partial
from itertools import chain
from datetime import datetime, timedelta
from os.path import join
import logging, csv
logger = logging.getLogger(__name__)



def getBloombergReconFiles(directory):
	"""
	[String] directory => ([String] file1, [String] file2)
	"""
	logger.debug('getBloombergReconFiles(): {0}'.format(directory))

	def checkDates(d1, d2):
		"""
		d1 can be Mon, Tue, Wed, Thu, Fri
		d2 can be Mon, Tue, Wed, Thu, Sun

		when d1, d2 are equal, they must Mon, Tue, Wed, or Thu
		when d1, d2 are not equal, then d1 must be Fri, d2 must be Sun
		and d2 - d1 must be 2 days.

		If either of the above are satisfied, then return True,
		else return False.
		"""
		logger.debug('checkDates(): {0}, {1}'.format(d1, d2))

		d1 = datetime.strptime(d1, '%Y%m%d')
		d2 = datetime.strptime(d2, '%Y%m%d')

		if d1 == d2 and d1.weekday() in (0, 1, 2, 3):
			return True

		if d1 != d2 and d1.weekday() == 4 and d2.weekday() == 6 and (d2 - d1).days == 2:
			return True

		return False


	files = compose(
		list
	  , partial(filter, lambda fn: fn.startswith('Repo_PosRecon_'))
	  , getFiles
	)(directory)

	F1 = list(filter(lambda fn: fn.endswith('_1.csv'), files))
	F2 = list(filter(lambda fn: fn.endswith('_2.csv'), files))

	if len(F1) == len(F2) and len(F1) == 1 and \
		checkDates(getDateFromFilename(F1[0]), getDateFromFilename(F2[0])):

		return (join(directory, F1[0]), join(directory, F2[0]))

	else:
		logger.error('getBloombergReconFiles(): invalid number of files')
		raise ValueError



"""
	[String] file => [String] date (yyyymmdd)

	From Bloomberg recon file, get the T day date.

	The file name looks like: Repo_PosRecon_20210309_1.csv
"""
getDateFromFilename = compose(
	lambda s: s[0:4] + s[4:6] + s[6:8]
  , lambda s: s.split('_')[-2]
  , getFilenameWithoutPath
)



def loadRepoPosition(file1, file2):
	"""
	[String] Bloomberg recon file 1 (for closed positions)
	[String] Bloomberg recon file 2 (for open positions)
		=> [Iterable] ([Dictionary] repo position)
	
	For both files, take out positions whose loan amount = 0
	Then,

	from file 1, filter those positions in file 1 but not in file 2
	from file 2, filter those positions in file 2

	Combine them
	"""
	def getPositions(file):
		""" [String] file => [List] ([Dictionary] position) """
		logger.debug('loadRepoPosition(): {0}'.format(file))
		headers = ( 'RepoName', 'Account', 'LoanAmount', 'AccruedInterest'
				  , 'OpenDate', 'CloseDate', 'InterestRate')

		with open(file, newline='') as csvfile:
			spamreader = csv.reader(csvfile, delimiter=',', quotechar='|')
			return \
			compose(
				list
			  , partial( map
			  		   , lambda p: mergeDict( p
			  		   						, {'CloseDate': '99991231' if p['CloseDate'] == '' \
			  		   							else p['CloseDate']}
			  		   						)
			  		   )
			  , partial( map
					   , lambda p: mergeDict( p
					   						, { 'LoanAmount': float(p['LoanAmount'])
					   						  , 'AccruedInterest': float(p['AccruedInterest'])
					   						  , 'InterestRate': float(p['InterestRate'])
					   						  }
					   						)
					   )
			  , partial(map, dict)
			  , partial(map, lambda row: zip(headers, row))
			)(spamreader)


	# [Iterable] repo positions => [Set] repo names that still exist
	getExistingRepoNames = compose(
		set
	  , partial(map, lambda p: p['RepoName'])
	  , partial(filter, lambda p: p['LoanAmount'] != 0)
	)

	file1Positions = getPositions(file1)
	file2Positions = getPositions(file2)

	s1 = getExistingRepoNames(file1Positions)
	s2 = getExistingRepoNames(file2Positions)

	return chain( filter( lambda p: p['RepoName'] in (s1-s2)
						, file1Positions) # positions that close in T+1

				, filter( lambda p: p['RepoName'] in s2
						, file2Positions) # positions that still open in T+1
				)



def enrichPosition(repoData, position):
	"""
	[Dictionary] repoData, [Dictionary] position 
		=> [Iterable] ([Dictionary] enriched position)

	Because a Bloomberg repo position can have one or multiple collaterals,
	we allocate the accrued interest pro-rata to each collateral.
	"""
	logger.debug('enrichPosition(): {0}'.format(position['RepoName']))

	# [String] date (yyyy-mm-dd) => [String] date (yyyymmdd)
	changeDate = compose(
		lambda L: L[0] + L[1] + L[2]
	  , lambda s: s.split('-')
	)

	try:
		return \
		map( lambda t: \
				mergeDict( position
					 	 , { 'CollateralID': t[0]
					   	   , 'CollateralQuantity': t[1]
					   	   , 'LoanAmount': position['LoanAmount']*t[2]
					   	   , 'AccruedInterest': position['AccruedInterest']*t[2]
					   	   , 'OpenDate': changeDate(t[3])
					   	   }
					 	 )
		   , repoData[position['RepoName']]
		   )

	except:
		logger.error('enrichPosition(): failed on {0}'.format(position['RepoName']))
		return [mergeDict( position
					 	 , {'CollateralID': '', 'CollateralQuantity': 0}
					 	 )]



def getRepoData():
	"""
	[Dictionary] ([String] repo name -> 
				  [Iterable] ( [String] collateral Id
				  			 , [Float] quantity
				  			 , [Float] share of total loan amount
				  			 , [String] settle date
				  			 )
				 )

	"""
	logger.debug('getRepoData()')

	getCollateralInfo = compose(
		lambda t: \
			map( lambda collateral_t: ( collateral_t[0]
									  , collateral_t[1]
									  , collateral_t[2]/t[1]
									  , collateral_t[3]
									  ) 
			   , t[0]
			   )
	  , lambda L: (L, sum(map(lambda t: t[2], L)))
	  , list
	  , partial( map
	  		   , lambda p: ( p['CollateralID']
	  		   			   , p['Quantity']
	  		   			   , p['CollateralValue']
	  		   			   , p['SettleDate']
	  		   			   )
	  		   )
	)

	return \
	compose(
		partial(valmap, getCollateralInfo)
	  , partial(groupbyToolz, lambda p: p['RepoName'])
	)(getRepo())



def getAccruedInterest(date, position):
	"""
	[Dictionary] recon date (yyyymmdd)
	[Dictionary] position => [Float] accrued interest

	Bloomberg gives accrued interest of a repo position:

	1) up to the day for OPEN repo;
	2) whole term for fixed term repo.

	So if it's fixed term repo, we need to calculate the pro-rata
	share of the accrued interest up to the day of reconciliation.
	"""
	getDateTime = lambda s: \
		datetime.strptime(s, '%Y%m%d')

	# position, date => [Float] pro-rata ratio
	getDaysRatio = compose(
		lambda t: (t[1] + 1)/t[0]
	  , lambda t: ( (t[1] - t[0]).days
				  , (t[2] - t[0]).days
				  )
	  , lambda p, date: ( getDateTime(p['OpenDate'])
	  					, getDateTime(p['CloseDate'])
	  					, getDateTime(date) 
	  					)
	)

	return \
	position['AccruedInterest'] if position['CloseDate'] == '99991231' else \
	position['AccruedInterest'] if position['CloseDate'] <= date else \
	0 if position['OpenDate'] > date else \
	position['AccruedInterest'] * getDaysRatio(position, date)



def updateAccruedInterest(date, position):
	"""
	[Dictionary] position => [Dictionary] position

	update the accrued interest
	"""
	logger.debug('updateAccruedInterest(): {0}'.format(position['RepoName']))
	return \
	mergeDict( position
			 , {'AccruedInterest': getAccruedInterest(date, position)}
			 )



def createRepoReconFile(directory, date, positions):
	"""
	[Iterable] repo positions after enrichment
		=> [String] file

	Create a csv file from the repo positions in the directory.
	"""
	logger.debug('createRepoReconFile(): {0}'.format(date))

	headers = ( 'RepoName', 'Account', 'CollateralID', 'CollateralQuantity'
			  , 'LoanAmount', 'AccruedInterest', 'OpenDate', 'CloseDate'
			  , 'InterestRate')

	changeDate = lambda dt: dt[0:4] + '-' + dt[4:6] + '-' + dt[6:8]
	
	updatePosition = lambda p: \
		mergeDict( p
				 , { 'OpenDate': changeDate(p['OpenDate'])
				   , 'CloseDate': changeDate(p['CloseDate'])
				   }
				 )

	return \
	compose(
		partial(writeCsv, join(directory, 'Repo_PosRecon_WithHeader_{0}.csv'.format(date)))
	  , partial(chain, [headers])
	  , partial(map, partial(dictToValues, headers))
	  , partial(map, updatePosition)
	)(positions)



def doUpload(file):
	"""
	[String] file => [String] file

	Side effect: upload the recon file to SFTP
	"""
	logger.debug('doUpload(): {0}'.format(file))
	upload('A2GPosition', [file])
	return file



def sendReconNotification(status, message):
	"""
	[Int] status, [String] message

	Side effect: send an email notification about reconciliation status
	"""
	getSubject = lambda status: \
		'AIM reconciliation upload failed' if status == Constants.STATUS_ERROR else \
		'AIM reconciliation upload successful' if status == Constants.STATUS_SUCCESS \
		else 'AIM reconciliation upload with unknown conditions'

	sendNotificationEmail(getSubject(status), message)




if __name__ == "__main__":
	import logging.config
	logging.config.fileConfig('logging.config', disable_existing_loggers=False)
	
	"""
	Load Bloomberg AIM reconfiles, enrich it with collateral information
	and create a new recon file, then upload it and notify the user about
	the result.
	"""
	logger.debug('main(): start')
	bloombergReconFiles = []
	try:
		initializeDatastore('production')

		bloombergReconFiles = getBloombergReconFiles(getDataDirectory())
		outputFile = compose(
			doUpload
		  , partial( createRepoReconFile, getDataDirectory()
		  		   , getDateFromFilename(bloombergReconFiles[1])
		  		   )
		  , partial( map
		  		   , partial( updateAccruedInterest
		  		   			, getDateFromFilename(bloombergReconFiles[1]))
		  		   )
		  , chain.from_iterable
		  , partial(map, partial(enrichPosition, getRepoData()))
		  , lambda t: loadRepoPosition(t[0], t[1])
		)(bloombergReconFiles)

	except:
		logger.exception('main()')
		sendReconNotification(Constants.STATUS_ERROR, '')
		moveFiles( join(getDataDirectory(), 'Position_SENT')
				 , bloombergReconFiles)

	else:
		logger.debug('main(): success')
		sendReconNotification(Constants.STATUS_SUCCESS, outputFile)
		moveFiles( join(getDataDirectory(), 'Position_SENT')
				 , bloombergReconFiles + (outputFile, ))
