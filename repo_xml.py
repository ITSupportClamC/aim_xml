# coding=utf-8
#
# Read information from repo XML files
# 

from aim_xml.steven_tools import fileToLines, changeLines, getTrade \
								, getDeleteTrade, getFileName
from steven_utils.utility import mergeDict
from toolz.functoolz import compose
from functools import partial
import logging
logger = logging.getLogger(__name__)



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



def getRepoRerateFromFile(file):
	"""
	[String] file 
		=> [Iterable] ([Dictionary] repo rerate)
	"""
	logger.debug('getRepoRerateFromFile(): {0}'.format(file))

	getRateTable = lambda L: dict(zip(L[0], L[1]))

	return \
	compose(
		partial( map
		   , lambda d: mergeDict(d, {'TransactionType': d['transaction_type']}))
	  , partial( map
	  		   , lambda el: mergeDict(el, {'RateTable': getRateTable(el['RateTable'])}))
	  , getRawDataFromXML
	)(file)