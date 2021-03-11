# coding=utf-8
#
# Save Bloomberg repo master, trade, and rerate file into 
# database.
# 
from repo_data.data import initializeDatastore, clearRepoData
from repo_data.repo_datastore import saveRepoMasterFileToDB \
									, saveRepoTradeFileToDB \
									, saveRepoRerateFileToDB
from aim_xml.utility import getDataDirectory
from os.path import join
import logging
logger = logging.getLogger(__name__)



if __name__ == "__main__":
	import logging.config
	logging.config.fileConfig('logging.config', disable_existing_loggers=False)
	
	logger.debug('main(): start')
	# initializeDatastore('production')

	# files = [ 'RepoMaster_20210311_20210311225611'
	# 		]

	# for file in files:
	# 	print(saveRepoMasterFileToDB(join(getDataDirectory(), file + '.xml')))

	# files = [ 'RepoTrade_20210311_20210311225659'
	# 		]

	# for file in files:
	# 	print(saveRepoTradeFileToDB(join(getDataDirectory(), file + '.xml')))
