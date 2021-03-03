# coding=utf-8
#
# Read information from repo XML files and save them to datastore.
# 
# Since this modules uses the datastore functions such as addRepoMaster,
# make sure set the database mode (production or test) before calling 
# any of them.
# 

from aim_xml.steven_tools import fileToLines, changeLines, getTrade \
								, getDeleteTrade, getFileName
from steven_utils.utility import mergeDict
from repo_data.data import addRepoMaster, addRepoTransaction \
						, cancelRepoTransaction, closeRepoTransaction \
						, rerateRepoTransaction, initializeDatastore \
						, clearRepoData, getRepo
from toolz.functoolz import compose
from functools import partial
import logging
logger = logging.getLogger(__name__)



"""
	[Dictionary] functionMap (key -> function), [Dictionary] d
		=> [Dictionary] updated d (with keys in functionMap)

	Create a new dictionary from an existing dictionary d, with
	values of certain keys updated if that key is both in the
	function map and the original dictionary.

"""
# updateDictionaryWithFunction = lambda functionMap, d: \
# 	mergeDict(d, {key: functionMap[key](d[key]) for key in \
# 					functionMap.keys() & d.keys()})



# def getRepoMasterFromFile(file):
# 	"""
# 	[String] file 
# 		=> [Iterable] ([Dictionary] repo master)
# 	"""
# 	logger.debug('getRepoMasterFromFile(): {0}'.format(file))
# 	return getRawDataFromXML(file)



# def getRepoTradeFromFile(file):
# 	"""
# 	[String] file 
# 		=> [Iterable] ([Dictionary] repo trade)
# 	"""
# 	logger.debug('getRepoTradeFromFile(): {0}'.format(file))

# 	return \
# 	compose(
# 		partial( map
# 			   , lambda d: mergeDict(d, {'TransactionType': d['transaction_type']}))
																		  
# 	  , partial( map
# 			   , partial( updateDictionaryWithFunction
# 			   			, { 'Quantity': float
# 			  			  , 'Price': float
# 						  , 'NetCounterAmount': float
# 						  , 'Coupon': float
# 						  , 'LoanAmount': float
# 						  }
# 						)
# 			   )
# 	  , getRawDataFromXML
# 	)(file)



"""
	[String] file => [Iterable] ([Dictionary] raw XML data)

	Read an XML trade file (see samples), return an iterable object on data
	nodes from that file.
"""
getRawDataFromXML = compose(
	partial(map, lambda L: dict(zip(L[0], L[1])))
  , getTrade
  , changeLines
  , fileToLines
)



def getRepoTradeFromFile(file):
	"""
	[String] file 
		=> [Iterable] ([Dictionary] repo trade)
	"""
	logger.debug('getRepoTradeFromFile(): {0}'.format(file))

	return \
	compose(
		partial( map
			   , lambda d: mergeDict(d, {'TransactionType': d['transaction_type']}))
	  , getRawDataFromXML
	)(file)



"""
	[List] keys to be kept, [Dictionary] d
		=> [Dictionary] d
"""
keepKeys = lambda keys, d: \
	{k: d[k] for k in set(keys).intersection(set(d))}



def saveRepoMasterFileToDB(file):
	"""
	[String] repo master file (without Geneva header) 
		=> [Int] no. of master entries saved into datastore
	"""
	logger.debug('saveRepoMasterFileToDB(): {0}'.format(file))
	def addRepo(masterInfo):
		try:
			addRepoMaster(masterInfo)
			return 1
		except:
			logger.exception('saveRepoMasterFileToDB()')
			return 0


	return \
	compose(
		sum
	  , partial(map, addRepo)
	  , partial( map
	  		   , partial( keepKeys
	  		   			, ['Code', 'BifurcationCurrency', 'AccrualDaysPerMonth', 'AccrualDaysPerYear']))
	  , getRawDataFromXML
	)(file)



def saveRepoTradeFileToDB(file):
	"""
	[String] repo trade file (without Geneva header) 
		=> [Int] no. of trades saved into datastore
	"""
	logger.debug('saveRepoTradeFileToDB(): {0}'.format(file))
	def addRepo(trade):
		try:
			addRepoMaster(trade)
			return 1
		except:
			logger.exception('saveRepoTradeFileToDB()')
			return 0


	return \
	compose(
		sum
	  , partial(map, addRepo)
	  , partial( map
	  		   , partial( keepKeys
	  		   			, ['Code', 'BifurcationCurrency', 'AccrualDaysPerMonth', 'AccrualDaysPerYear']))
	  , getRawDataFromXML
	)(file)