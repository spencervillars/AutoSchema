from sys 				import argv, stdin
from os 				import walk
from itertools 			import product
from textblob 			import Word
from textblob.wordnet 	import NOUN
from statistics 		import median
from math import floor

import requests # e.g. requests.get('https://example.com/')
import fileinput
import argparse
import re

class AutoSchemaClassifier:

	def __init__ (self, minimum_set_similarity,
						url_to_class_list="", 
						url_to_sample="", 
						sample_size=5):
		self.minimum_set_similarity = minimum_set_similarity
		self.url_to_class_list 		= url_to_class_list
		self.url_to_sample 			= url_to_sample
		self.sample_size 			= sample_size


	def findClosestSet(self):
		#
		# if this returns "", then the min set similarity threshold
		# was not met. Server should realize this and default to string
		#

		classes = loadClassList()

		input_sample = stdin.readLines()
		classification = ""
		max_similarity = 0 	# handles the similarity acceptance threshold on server side?
		for c in classes:
			dict_sample = loadSample(c)
			spts = self.findShortestPathToSet(dict_sample, 
											  input_sample)
			if spts > max_similarity and spts >= self.minimum_set_similarity:
				max_similarity = spts
				classification = c

		return classification




	
	# TODO: display a lil' message if request didn't get any response?
	def loadClassList(self):
		#
		# loads the list of classes (a.k.a. 'dictionaries') on server.
		# 	e.g. of list: r.text = ['animals','names','locations']	
		#	
		r = requests.get(self.url_to_class_list)

		if r.code_status == requests.codes.ok: 
			return str.splitlines(r.text)
		else: 
			return []

	def loadSample(self, class_name):
		#
		# the GET request should be to url_to_sample +"/"+ class_name +"/?n=" + sample_size
		#

		rqst_url =  self.url_to_sample + "/" + class_name
		smpl_size = str( self.sample_size )

		r = requests.get( rqst_url, 
						  params={ 'n' : smpl_size })

		sts = r.code_status
		oki = requests.codes.ok
		if (sts == oki): 
			return str.splitlines(r.text)
		else: 								   
			return []


	def findShortestPathToSet(self, dict_sample, input_sample):
		#
		# find the shortest sematic path between a dictionary list and a input list
		#
		def getBestPath(wordA, wordB):
			# TODO. make this waay better
			synA = Word(re.sub(r"\s+", '_', wordA)).get_synsets(pos=NOUN)
			synB = Word(re.sub(r"\s+", '_', wordB)).get_synsets(pos=NOUN)
			synProduct = product(synA,synB)

			max_similarity = 0
			for s in synProduct:
				cur_similarity = float(s[0].path_similarity(s[1]))
				if  cur_similarity > max_similarity:
					max_similarity = cur_similarity

			return  max_similarity

		def getShortestPath(wordA, wordB):
			return getBestPath(str(wordA), str(wordB))

		combos = list(product(dict_sample, input_sample))

		paths = []
		for c in combos:
			paths.append(getShortestPath(c[0],c[1]))

		#paths.sort()
		ret = 0
		if len(paths):
			ret = median(paths) + floor(1000*max(paths)) #sum( p * (p > self.minimum_set_similarity) for p in paths)
		return ret
