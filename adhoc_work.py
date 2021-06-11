# coding=utf-8
#
# Not for scheduling purpose, but to get some adhoc work done.
# 
from repo_data.data import initializeDatastore, getRepo
import logging
logger = logging.getLogger(__name__)



def search_ticket_db(ticket):
	"""
	Search for a ticket# from the database.
	"""
	result = list(filter( lambda p: p['TransactionId'] == ticket
						, getRepo(status='all')))
	if result == []:
		print('ticket {0} not found'.format(ticket))
	elif len(result) > 1:
		print('multiple entries for ticket {0} found'.format(ticket))
	else:
		print(result[0])



if __name__ == "__main__":
	import logging.config
	logging.config.fileConfig('logging.config', disable_existing_loggers=False)
	
	initializeDatastore('production')
	search_ticket_db('328547')