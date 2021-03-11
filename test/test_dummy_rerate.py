# coding=utf-8
# 

import unittest2
from aim_xml.repo_xml import getRepoTradeFromFile, getRepoRerateFromFile
from aim_xml.dummy_rerate import createDummyRerateFile
from aim_xml.utility import getCurrentDir
from os.path import join



class TestDummyRerate(unittest2.TestCase):

	def __init__(self, *args, **kwargs):
		super(TestDummyRerate, self).__init__(*args, **kwargs)



	def testCreateDummyFile1(self):
		inputFile = join(getCurrentDir(), 'samples', 'RepoTrade_20210216.xml')
		outputFile = createDummyRerateFile(inputFile)
		self.assertEqual('', outputFile)



	def testCreateDummyFile2(self):
		inputFile = join(getCurrentDir(), 'samples', 'RepoTrade_sample_mixed.xml')
		outputFile = createDummyRerateFile(inputFile)
		self.assertEqual('Dummy_ReRate_RepoTrade_sample_mixed.xml', outputFile)
		self.verifyFile(join(getCurrentDir(), 'samples', outputFile))



	def verifyFile(self, file):
		positions = list(getRepoRerateFromFile(file))
		self.assertEqual(2, len(positions))

		self.assertEqual('UserTranId1=313562', positions[0]['Loan'])
		data = positions[0]['RateTable']
		self.assertEqual('Insert', data['CommitType'])
		self.assertEqual('2021-01-26T00:00:00', data['RateDate'])
		self.assertEqual('1.1', data['Rate'])

		self.assertEqual('UserTranId1=309636', positions[1]['Loan'])
		data = positions[1]['RateTable']
		self.assertEqual('Insert', data['CommitType'])
		self.assertEqual('2021-02-11T00:00:00', data['RateDate'])
		self.assertEqual('1.2', data['Rate'])
