# coding=utf-8
#
# Read information from repo XML files and save them to datastore.
# 

from aim_xml.steven_tools import fileToLines, changeLines, getTrade \
								, getDeleteTrade, getFileName
from steven_utils.utility import mergeDict
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
updateDictionaryWithFunction = lambda functionMap, d: \
	mergeDict(d, {key: functionMap[key](d[key]) for key in \
					functionMap.keys() & d.keys()})



def getRepoMasterFromFile(file):
	"""
	[String] file 
		=> [Iterable] ([Dictionary] repo master)
	"""
	logger.debug('getRepoMasterFromFile(): {0}'.format(file))
	return getRawDataFromXML(file)



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
																		  
	  , partial( map
			   , partial( updateDictionaryWithFunction
			   			, { 'Quantity': float
			  			  , 'Price': float
						  , 'NetCounterAmount': float
						  , 'Coupon': float
						  , 'LoanAmount': float
						  }
						)
			   )
	  , getRawDataFromXML
	)(file)



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