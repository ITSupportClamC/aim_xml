# coding=utf-8
# 

import unittest2
from aim_xml.repo_datastore import saveRepoMasterFileToDB
from aim_xml.utility import getCurrentDir
from repo_data.constants import Constants
from repo_data.data import initializeDatastore, clearRepoData
from steven_utils.utility import mergeDict
from toolz.functoolz import compose
from functools import partial
from os.path import join, dirname, abspath



class TestDatastore(unittest2.TestCase):

	def __init__(self, *args, **kwargs):
		super(TestDatastore, self).__init__(*args, **kwargs)


	def setUp(self):
		initializeDatastore('test')
		clearRepoData()


	def testAddRepoMaster(self):
		inputFile = join(getCurrentDir(), 'samples', 'RepoMaster_20210218_20210218121101.xml')
		self.assertEqual(2, saveRepoMasterFileToDB(inputFile))
		

