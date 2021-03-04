# coding=utf-8
# 

import unittest2
from aim_xml.repo_data import getRepoTradeFromFile, getRawDataFromXML
from aim_xml.utility import getCurrentDir
from os.path import join



class TestRepoDatastore(unittest2.TestCase):

	def __init__(self, *args, **kwargs):
		super(TestRepoDatastore, self).__init__(*args, **kwargs)


	def testRepoMasterFromFile(self):
		inputFile = join(getCurrentDir(), 'samples', 'RepoMaster_20210210.xml')
		info = list(getRawDataFromXML(inputFile))
		self.assertEqual(5, len(info))
		self.verifyMaster(info[4])



	def testRepoTradeFromFile(self):
		inputFile = join(getCurrentDir(), 'samples', 'RepoTrade_20210209.xml')
		info = list(getRepoTradeFromFile(inputFile))
		self.assertEqual(6, len(info))
		self.verifyTrade1(info[0])
		self.verifyTrade2(info[1])
		self.verifyTrade3(info[2])



	def testRepoTradeFromFile2(self):
		inputFile = join(getCurrentDir(), 'samples', 'RepoTrade_20210216.xml')
		info = list(getRepoTradeFromFile(inputFile))
		self.assertEqual(1, len(info))
		trade = info[0]
		self.assertEqual('Repo_InsertUpdate', trade['TransactionType'])
		self.assertEqual('309790', trade['UserTranId1'])
		self.assertEqual('2021-02-16T00:00:00', trade['ActualSettleDate'])
		self.assertEqual('', trade['OpenEnded'])
		self.assertEqual('CALC', trade['FundStructure'])
		self.assertEqual('CALC', trade['AccruedInterest'])



	def testRepoTradeFromFile3(self):
		inputFile = join(getCurrentDir(), 'samples', 'RepoTrade_20210216_2.xml')
		info = list(getRepoTradeFromFile(inputFile))
		self.assertEqual(1, len(info))
		trade = info[0]
		self.assertEqual('Repo_InsertUpdate', trade['TransactionType'])
		self.assertEqual('309790', trade['UserTranId1'])
		self.assertEqual('2021-02-16T00:00:00', trade['ActualSettleDate'])
		self.assertEqual('', trade['OpenEnded'])
		self.assertEqual('', trade['Hello'])
		self.assertEqual('CALC', trade['FundStructure'])
		self.assertEqual('CALC', trade['AccruedInterest'])



	def verifyMaster(self, master):
		self.assertEqual('MMRPE121ST', master['Code'])
		self.assertEqual('USD', master['BifurcationCurrency'])
		self.assertEqual('Actual', master['AccrualDaysPerMonth'])
		self.assertEqual('360', master['AccrualDaysPerYear'])



	def verifyTrade1(self, trade):
		self.assertEqual('Repo_Delete', trade['TransactionType'])
		self.assertEqual('309096', trade['UserTranId1'])
		self.assertEqual('CALC', trade['FundStructure'])



	def verifyTrade2(self, trade):
		self.assertEqual('ReverseRepo_InsertUpdate', trade['TransactionType'])
		self.assertEqual('309096', trade['UserTranId1'])
		self.assertEqual('2021-02-09T00:00:00', trade['ActualSettleDate'])
		self.assertEqual('CALC', trade['FundStructure'])
		self.assertEqual('CALC', trade['AccruedInterest'])



	def verifyTrade3(self, trade):
		self.assertEqual('309633', trade['UserTranId1'])
		self.assertEqual('TEST_R', trade['Portfolio'])
		self.assertEqual('BOCHK', trade['LocationAccount'])
		self.assertEqual('Isin=XS2178949561', trade['Investment'])
		self.assertEqual('2021-02-09T00:00:00', trade['EventDate'])
		self.assertEqual('2021-02-09T00:00:00', trade['SettleDate'])
		self.assertEqual('2021-03-11T00:00:00', trade['ActualSettleDate'])
		self.assertEqual('114494.03', trade['Quantity'])
		self.assertEqual('USD', trade['CounterInvestment'])
		self.assertEqual('108.096', trade['Price'])
		self.assertEqual('100000.0', trade['NetCounterAmount'])
		self.assertEqual('MMRPE3221Q', trade['RepoName'])
		self.assertEqual('Default', trade['Strategy'])
		self.assertEqual('0.95', trade['Coupon'])
		self.assertEqual('100000.0', trade['LoanAmount'])
		self.assertEqual('BNP-REPO', trade['Broker'])
		self.assertEqual('Actual', trade['AccrualDaysPerMonth'])
		self.assertEqual('360', trade['AccrualDaysPerYear'])
		self.assertEqual('CALC', trade['AccruedInterest'])
		self.assertEqual('CALC', trade['FundStructure'])