from sys 				import argv, stdin
from os 				import walk
from itertools 			import product
from textblob 			import Word
from textblob.wordnet 	import NOUN
from statistics 		import median
from math import floor

#import requests # e.g. requests.get('https://example.com/')
import fileinput
import argparse
import re

class AutoSchemaClassifier:

	def __init__ (self, minimum_set_similarity):
		#TODO: should the min_set_similarity be handled in here? or in sql execute?
		self.minimum_set_similarity = minimum_set_similarity

	#
	# find the shortest sematic path between a dictionary list and a input list
	#
	def findShortestPathToSet(self, dict_sample, input_sample):
		#
		# the 'best' path is the one with the highest similarity.
		# e.g.: 'weasel' may have multiple meanings, but we will
		#   pick the definition that has the highest similarity 
		#   to our word from the input sample
		# 
		def getBestPath(wordA, wordB):

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

		ret = 0
		if len(paths):
			ret = median(paths) + floor(1000*max(paths)) #sum( p * (p > self.minimum_set_similarity) for p in paths)
		return ret


	#
	# finds the best semantic column match from a selections of columns.
	# e.g.: we want to see if the 'string_to_classify' = fox
	#		is more likely to appear in column 1 or column 2 ... etc.
	#
	# this function returns a list of the similarities
	#
	def computeSimilarityOfStringToCloumns(self, known_column_samples, string_to_classify):

		similarities = []		
		num_cols = len(known_column_samples)

		for c in known_column_samples:
			t = findShortestPathToSet([string_to_classify], known_column_samples[c])
			similarity.append(t)

		return similarities
