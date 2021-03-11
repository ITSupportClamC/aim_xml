# coding=utf-8
# 

import unittest2
from aim_xml.repo_recon import getBloombergReconFiles, loadRepoPosition \
							, enrichPosition, getRepoData, getDateFromFilename \
							, updateAccruedInterest
from aim_xml.utility import getCurrentDir
from repo_data.data import initializeDatastore, clearRepoData
from repo_data.repo_datastore import saveRepoMasterFileToDB \
									, saveRepoTradeFileToDB \
									, saveRepoRerateFileToDB
from steven_utils.file import getFilenameWithoutPath
from toolz.functoolz import compose
from functools import partial
from itertools import chain
from os.path import join



class TestRepoRecon(unittest2.TestCase):

	def __init__(self, *args, **kwargs):
		super(TestRepoRecon, self).__init__(*args, **kwargs)
		initializeDatastore('test')



	def testGetBloombergReconFiles(self):
		directory = join(getCurrentDir(), 'samples')
		file1, file2 = getBloombergReconFiles(directory)
		self.assertEqual('Repo_PosRecon_20210309_1.csv', getFilenameWithoutPath(file1))
		self.assertEqual('Repo_PosRecon_20210309_2.csv', getFilenameWithoutPath(file2))



	def testLoadRepoPosition(self):
		file1 = join(getCurrentDir(), 'samples', 'Repo_PosRecon_20210309_1.csv')
		file2 = join(getCurrentDir(), 'samples', 'Repo_PosRecon_20210309_2.csv')
		L = list(loadRepoPosition(file1, file2))
		self.assertEqual(9, len(L))
		self.assertEqual( set([ 'MMRPEA256T', 'MMRPE1257V', 'MMRPEB24DV', 'MMRPEA2560'
							  , 'MMRPE925N6', 'MMRPE925MV', 'MMRPE425RN', 'MMRPE12574'
							  , 'MMRPE322ZO'
							  ])
						, set(map(lambda p: p['RepoName'], L))
						)



	def testEnrichPosition(self):
		"""
		To complete this test, load information into database first,
		then enrich repo position with database info.
		"""
		clearRepoData()
		files = [ 'RepoMaster_20210309_20210309165806'
				, 'RepoMaster_20210309_20210309190550'
				, 'RepoMaster_20210309_20210309204006'
				, 'RepoMaster_20210310_20210310225909'
				]

		for file in files:
			saveRepoMasterFileToDB(join( getCurrentDir(), 'samples'
									   , file + '.xml'))

		files = [ 'RepoTrade_20210309_20210309174113'
				, 'RepoTrade_20210309_20210309190913'
				, 'RepoTrade_20210309_20210309204024'
				, 'RepoTrade_20210310_20210310230717'
				]

		for file in files:
			saveRepoTradeFileToDB(join( getCurrentDir(), 'samples'
									  , file + '.xml'))

		saveRepoRerateFileToDB(join( getCurrentDir(), 'samples'
								   , 'Repo_ReRate_20210309_20210309174543.xml'))

		file1 = join(getCurrentDir(), 'samples', 'Repo_PosRecon_20210309_1.csv')
		file2 = join(getCurrentDir(), 'samples', 'Repo_PosRecon_20210309_2.csv')
		L = compose(
			list
		  , partial(map, partial(updateAccruedInterest, getDateFromFilename(file1)))
		  , chain.from_iterable
		  , partial(map, partial(enrichPosition, getRepoData()))
		  , loadRepoPosition
		)(file1, file2)

		self.assertEqual(10, len(L))
		self.verifyEnrichedPosition1(L) # closed position
		self.verifyEnrichedPosition2(L) # multi collateral, type OPEN
		self.verifyEnrichedPosition3(L) # fixed term
		self.verifyEnrichedPosition4(L) # not started yet



	def verifyEnrichedPosition1(self, L):
		position = list(filter(lambda p: p['RepoName'] == 'MMRPEA256T', L))[0]
		self.assertEqual('MMRPEA256T', position['RepoName'])
		self.assertEqual('TEST_R', position['Account'])
		self.assertEqual(-280000, position['LoanAmount'])
		self.assertEqual(-218.17, position['AccruedInterest'])
		self.assertEqual('20210205', position['OpenDate'])
		self.assertEqual('20210310', position['CloseDate'])
		self.assertEqual(0.85, position['InterestRate'])
		self.assertEqual('HK0000163607', position['CollateralID'])
		self.assertEqual(350000, position['CollateralQuantity'])



	def verifyEnrichedPosition2(self, L):
		"""
		For details of this multi collateral ticket, please refer to
		file "RepoTrade_20210309_20210309190913.xml" in samples directory.
		"""
		L = list(filter(lambda p: p['RepoName'] == 'MMRPE925N6', L))
		self.assertEqual(2, len(L))

		L = sorted(L, key=lambda p: abs(p['LoanAmount']))

		position = L[0]
		self.assertEqual('MMRPE925N6', position['RepoName'])
		self.assertEqual('TEST_R', position['Account'])
		self.assertEqual(-100000, position['LoanAmount'])
		self.assertAlmostEqual(4.8, position['AccruedInterest'], 6)
		self.assertEqual('20210309', position['OpenDate'])
		self.assertEqual('99991231', position['CloseDate'])
		self.assertEqual(0.95, position['InterestRate'])
		self.assertEqual('XS2178949561', position['CollateralID'])
		self.assertEqual(120000, position['CollateralQuantity'])

		position = L[1]
		self.assertEqual('MMRPE925N6', position['RepoName'])
		self.assertEqual('TEST_R', position['Account'])
		self.assertEqual(-400000, position['LoanAmount'])
		self.assertAlmostEqual(19.2, position['AccruedInterest'], 6)
		self.assertEqual('20210309', position['OpenDate'])
		self.assertEqual('99991231', position['CloseDate'])
		self.assertEqual(0.95, position['InterestRate'])
		self.assertEqual('XS2282244560', position['CollateralID'])
		self.assertEqual(500000, position['CollateralQuantity'])



	def verifyEnrichedPosition3(self, L):
		position = list(filter(lambda p: p['RepoName'] == 'MMRPEB24DV', L))[0]
		self.assertEqual('MMRPEB24DV', position['RepoName'])
		self.assertEqual('TEST_R', position['Account'])
		self.assertEqual(250000, position['LoanAmount'])
		self.assertAlmostEqual(38.690625, position['AccruedInterest'], 6)
		self.assertEqual('20210304', position['OpenDate'])
		self.assertEqual('20210405', position['CloseDate'])
		self.assertEqual(0.65, position['InterestRate'])
		self.assertEqual('HK0000142494', position['CollateralID'])
		self.assertEqual(300000, position['CollateralQuantity'])



	def verifyEnrichedPosition4(self, L):
		position = list(filter(lambda p: p['RepoName'] == 'MMRPE425RN', L))[0]
		self.assertEqual('MMRPE425RN', position['RepoName'])
		self.assertEqual('TEST_R', position['Account'])
		self.assertEqual(-702581.91, position['LoanAmount'])
		self.assertAlmostEqual(0, position['AccruedInterest'], 6)
		self.assertEqual('20210331', position['OpenDate'])
		self.assertEqual('20210430', position['CloseDate'])
		self.assertEqual(0.80, position['InterestRate'])
		self.assertEqual('XS1897158892', position['CollateralID'])
		self.assertEqual(770000, position['CollateralQuantity'])