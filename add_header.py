# coding=utf-8
#
# Functions related to Bloomberg AIM XML trade file.
# 
from aim_xml.utility import getCurrentDir, getDatetimeAsString
from steven_utils.file import getFilenameWithoutPath, getParentFolder
from toolz.functoolz import compose
from itertools import filterfalse, chain
from functools import partial
from os.path import join
import logging
logger = logging.getLogger(__name__)



def fileToLines(file):
	"""
	[String] file => [List] lines in the file

	read a file in text mode and returns all its lines
	"""
	lines = []
	with open(file, 'r') as f:
		lines = f.read().splitlines()

	return lines



# [String] file => [Bool] is the file a repo master file
isRepoMaster = compose(
	lambda file: \
		file.lower().startswith('repomaster') and not 'WithHeaders' in file
  , getFilenameWithoutPath
)



# [String] file => [Bool] is the file a repo trade file
isRepoTrade = compose(
	lambda file: \
		file.lower().startswith('repotrade') and not 'WithHeaders' in file
  , getFilenameWithoutPath
)



# [String] file => [Bool] is the file a repo rerate file
isRepoRerate = compose(
	lambda file: \
		file.lower().startswith('reporerate') and not 'WithHeaders' in file
  , getFilenameWithoutPath
)



# [String] file => [Bool] is the file a repo resize file
isRepoResize = compose(
	lambda file: \
		file.lower().startswith('reporesize') and not 'WithHeaders' in file
  , getFilenameWithoutPath
)



def addRepoHeaders(file):
	"""
	[String] file => [String] output file

	Assume: the input file is a repo XML file of 3 types:
	repo loan master file, repo transaction file, repo rerate file

	The function reads the input XML file, add appropriate Geneva headers to
	its content and saves the output file into the same folder.
	"""
	logger.debug('addRepoHeaders(): {0}'.format(file))

	# [String] file => [String] file type
	getFileTypeFromName = lambda file: \
		'loan_master' if isRepoMaster(file) else \
		'transaction' if isRepoTrade(file) else \
		'rerate' if isRepoRerate(file) else 'others'


	# [String] file => [Tuple] (headers, footers)
	getHeaderForFile = compose(
		getRepoHeaders
	  , getFileTypeFromName
	)


	def getOutputFilename(file):

		getOutputFile = compose(
			lambda t: t[0] + '_WithHeaders_' + getDatetimeAsString() + t[1]
		  , lambda file: (file[0:-4], file[-4:])
		  , getFilenameWithoutPath
		)

		return join(getParentFolder(file), getOutputFile(file))
	# end of getOutputFilename()


	def writeLinesToFile(lines, fileName):
		with open(fileName, 'w') as f:
			f.writelines(lines)

		return fileName
	# end of writeLinesToFile()


	return \
	compose(
		lambda lines: writeLinesToFile(lines, getOutputFilename(file))
	  , lambda t: chain(t[1][0], t[0], t[1][1])
	  , lambda file: ( fileToLines(file)
	  				 , getHeaderForFile(file))
	)(file)



def getRepoHeaders(fileType):
	"""
	[String] file type: one of the 3: loan_master, transaction, rerate

		=> [Tuple] ([Iterable] header lines, [Iterable] footer lines)

	Assume: the xml header files consist of 4 lines, except comments and
	blank lines, 2 for headers and 2 for footers.
	"""
	def getXmlHeaderFile(fileType):
		if fileType == 'loan_master':
			return join(getCurrentDir(), 'data', 'repo_master_header.txt')
		elif fileType == 'transaction':
			return join(getCurrentDir(), 'data', 'repo_trade_header.txt')
		elif fileType == 'rerate':
			return join(getCurrentDir(), 'data', 'repo_rerate_header.txt')
		else:
			logger.error('getXmlHeaderFile(): invalid file type {0}'.format(fileType))
			raise ValueError

	
	def check4lines(L):
		if len(L) == 4:
			return L
		else:
			logger.error('getRepoHeaders(): check4lines(): invalid length: {0}'.format(len(L)))
			raise ValueError


	return \
	compose(
		lambda L: ((L[0], L[1]), (L[2], L[3]))
	  , check4lines
	  , list
	  , partial(filterfalse, lambda line: line == '' or line[0] == '#')
	  , partial(map, lambda line: line.strip())
	  , fileToLines
	  , getXmlHeaderFile
	)(fileType)




if __name__ == "__main__":
	import logging.config
	logging.config.fileConfig('logging.config', disable_existing_loggers=False)
	
	import argparse
	parser = argparse.ArgumentParser()
	parser.add_argument('file', metavar='input XML file', type=str, help="XML file name")
	args = parser.parse_args()

	"""
	To test how addRepoHeaders() function works, put an repo xml file without
	headers into the local directory, then run:

		$ python add_header.py <file name>

	You should see the output file in the same directory.
	"""
	print('output file: {0}'.format(addRepoHeaders(args.file)))
