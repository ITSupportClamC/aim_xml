# coding=utf-8
# 

import unittest2
from aim_xml.repo_recon import getBloombergReconFiles, loadRepoPosition
from steven_utils.file import getFilenameWithoutPath
from aim_xml.utility import getCurrentDir
from os.path import join



class TestRepoRecon(unittest2.TestCase):

	def __init__(self, *args, **kwargs):
		super(TestRepoRecon, self).__init__(*args, **kwargs)


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
							  , 'MMRPE925N6', 'MMRPE925MV', 'MMRPE425RN', 'MMRPE322ZO'
							  , 'MMRPE12574'])
						, set(map(lambda p: p['RepoName'], L))
						)