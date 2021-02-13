# coding=utf-8
#
# Functions related to Bloomberg AIM XML trade file.
# 

from aim_xml.steven_tools import extractTradeFile, getTrade, getDeleteTrade \
								, getFileName, printReadable
import logging
logger = logging.getLogger(__name__)



def getRepoInfoFromFile(updater, file):
	"""
	[Function] ([Dictionary] -> [Dictionary]) info updater,
	[String] file 
		=> [Iterable] repo trades
	"""
	logger.debug('')
	return map(updater, getTradesFromFile(file))



def getTradesFromFile(file):
	"""
	[String] file => [Iterable] trades

	Read an XML trade file (see samples), return an iterable object on all
	the trades from that file, where each trade is represented by a dictionary 
	object. For example, for samples/sample01.xml, we should have below:

	trades = list(getTradesFromFile('samples01.xml'))

	len(trades) == 1
	trades[0]['transaction_type'] == 'Sell_New'
	trades[0]['Portfolio'] == '30001'
	...
	trades[0]['TradeExpenses']['ExpenseAmt'] == ''
	...
	
	"""
	return getTrade(extractTradeFile(file))



def getTradesAfterDeletion(trades):
	"""
	[Iterable] trades => [Iterable] trades
	
	From an iterable object of trades, find the deletions, then remove those
	trades removed by the deletion, return the remaining trades.
	
	The deletion element looks like, where "Sell_Delete" can be of other form,
	like "Buy_Delete" or "XX_delete" (XX is any string without space)

	<Sell_Delete>
		<KeyValue>280064</KeyValue>
	</Sell_Delete>

	There is a key value inside a deletion element. For each "normal" trade,
	there is also a key value inside. So when a deletion element has the same
	key value as another trade, that means the trade is deleted.
	"""
	return getDeleteTrade(trades)



def main():
	"""

	"""
	import argparse
	import os
	parser = argparse.ArgumentParser()
	parser.add_argument('--file', nargs='?', metavar='input XML file', type=str, help="XML file name")
	parser.add_argument('-d', '--debug', help='print readable', action='store_true')
	parser.add_argument('-wd', '--withdeletion', help='Get Trades After Deletion', action='store_true')

	args = parser.parse_args()
	if (args.file == None):
		parser.print_help()
		return

	else:
		filename = getFileName(args.file)

	if os.path.isfile(filename):
		trades = getTradesFromFile(filename)
		if args.withdeletion:
			tradesAfterDeleteion = getTradesAfterDeletion(trades)
			if args.debug:
				printReadable(tradesAfterDeleteion)

		else:
			if args.debug:
				printReadable(trades)
	else: 
		print(filename + " file not found!")




if __name__ == "__main__":
	import logging.config
	logging.config.fileConfig('logging.config', disable_existing_loggers=False)
	main()