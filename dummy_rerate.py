# coding=utf-8
#
# Read Repo trade file, and create dummy rerate file from it.
# 
from aim_xml.repo_data import getRepoTradeFromFile
from aim_xml.repo_upload import getFilesByType
from steven_utils.file import getFilenameWithoutPath, getParentFolder
from toolz.functoolz import compose
from functools import partial
from os.path import join
import logging
logger = logging.getLogger(__name__)



"""
	1 Get all repo trades of type OPEN;
	2 Create string of text (variable rate record) for each trade;
	3 Save all text into text file.
"""
def getRepoOpenTrades(file):
	"""
	[String] file => [Iterable] trades
	"""
	"""
	get all trades
	filter only open repo trade
	"""
	logger.debug('getRepoOpenTrades(): {0}'.format(file))

	repoOpenTrade = lambda t: \
		t['TransactionType'] in ('ReverseRepo_InsertUpdate', 'Repo_InsertUpdate') \
		and 'LoanAmount' in t and t['ActualSettleDate'] == 'CALC'


	return \
	compose(
		partial(filter, repoOpenTrade)
	  , getRepoTradeFromFile
	)(file)



def toXMLString(trade):
	"""
	[Dictionary] repo trade of type OPEN => [String] text
	"""
	return \
	'<VariableRateRecord>\n' + '<Loan>UserTranId1={0}</Loan>\n'.format(trade['UserTranId1']) + \
	'<RateTable>\n<CommitType>Insert</CommitType>\n' + \
	'<RateDate>{0}</RateDate>\n'.format(trade['SettleDate']) + \
	'<Rate>{0}</Rate>\n'.format(trade['Coupon']) + \
	'</RateTable>\n</VariableRateRecord>'



def getOutputFilename(file):
	"""
	[String] file => [String] input file

	Derive the output file name from the input file
	"""
	return 'Dummy_ReRate_' + getFilenameWithoutPath(file)



def toFile(outputDir, file, text):
	"""
	[String] output directory, [String] output file, [String] text 
		=> [String] output file (with full path)

	side effect: create an XML file in the output directory
	"""
	logger.debug('toFile(): {0}'.format(file))
	with open(join(outputDir, file), 'w') as f:
		f.write(text)

	return file



"""
	[String] input file => [String] output file

	Read a repo trade file, produce a dummy rerate file for each of the new
	repo position of type OPEN. return the file name. If there is no such 
	repo position, then no output file is created and return an empty string.

	Assume: the input repo trade file is without the Geneva headers.
"""
createDummyRerateFile = lambda file: \
compose(
	lambda s: \
		toFile(getParentFolder(file), getOutputFilename(file), s) if s != '' else ''

  , lambda L: '\n'.join(L)
  , partial(map, toXMLString)
  , getRepoOpenTrades
)(file)




if __name__ == "__main__":
	import logging.config
	logging.config.fileConfig('logging.config', disable_existing_loggers=False)
	
	import argparse
	parser = argparse.ArgumentParser(description='Create dummy repo rerate files')
	parser.add_argument('file', metavar='file', type=str, help="repo trade XML file")

	"""
	Search for trade files in the data directory (see config file), for each
	trade file there, produce dummy rerate file if necessary 
	"""
	logger.debug('main:')
	for file in getFilesByType('trade'):
		logger.debug('main: process trade file: {0}'.format(file))
		logger.debug('main: create dummy rerate file: {0}'.format(createDummyRerateFile(file)))
