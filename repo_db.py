# coding=utf-8
#
# Save Bloomberg repo master, trade, and rerate information into 
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
	initializeDatastore('test')
	clearRepoData()
	# files = [ 'RepoMaster_20210309_20210309165806'
	# 		, 'RepoMaster_20210309_20210309190550'
	# 		, 'RepoMaster_20210309_20210309204006'
	# 		, 'RepoMaster_20210310_20210310225909'
	# 		]

	# for file in files:
	# 	print(saveRepoMasterFileToDB(join(getDataDirectory(), file + '.xml')))

	# files = [ 'RepoTrade_20210309_20210309174113'
	# 		, 'RepoTrade_20210309_20210309190913'
	# 		, 'RepoTrade_20210309_20210309204024'
	# 		, 'RepoTrade_20210310_20210310230717' 
	# 		]

	# for file in files:
	# 	print(saveRepoTradeFileToDB(join(getDataDirectory(), file + '.xml')))


	print(saveRepoRerateFileToDB(join(getDataDirectory(), 'Repo_ReRate_20210309_20210309174543.xml')))