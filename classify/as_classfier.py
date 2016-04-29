#!/usr/bin/python3

# libaries I might not need:
# import pickle

# libaries I will use:

import fileinput
import os
import sys

from os import walk
from sys import argv, stdin

from itertools import product

from textblob import Word
from textblob.wordnet import NOUN

import requests
#e.g. requests.get('https://example.com/')

from statistics import median

import argparse

class AutoSchemaClassifier:



	def __init__ (self, url_to_class_list, url_to_sample, url_to_input, sample_size):
		self.url_to_class_list = url_to_class_list
		self.url_to_sample = url_to_sample
		self.url_to_input = url_to_input
		self.sample_size = sample_size

	# this is is for loading the list of classes a.k.a. 'dictionaries' availible on the server
	def loadClassList():
		r = requests.get(self.url_to_class_list)


		if r.code_status == requests.codes.ok: return str.splitlines(r.text)
		else								 : return []

	def loadSample():
		#
		# the GET request should be to url_to_sample +"/"+ class_name +"/?n=" + sample_size
		#
		request_url =  self.url_to_sample + "/" + self.class_name

		r = requests.get( request_url, params={'n':str( self.sample_size )} )

		if r.code_status == requests.codes.ok: return str.splitlines( r.text )
		else: 								   return []



	def findShortestPathToSet(dict_sample, target_set):
		def getBestPath(wordA, wordB):
			# TODO. make this waay better
			synA = wordA.synsets[0]
			synB = wordB.synsets[0]
			return synA.path_similarity(synB)

		def getShortestPath(two_words) :
			wordA = Word(str(two_words[0]))
			wordB = Word(str(two_words[1]))
			return getBestPath(wordA, wordB)

		combos = list(product(dict_sample, target_set))

		paths = []
		for c in combos:
			paths.append(getShortestPath(c))
		return median(c)



def test_as_classifier():
	parser = argparse.ArgumentParser(description="process args")
	parser.add_argument("dictdir")

	# if we wanted to add a flag argument, this is how we would do it 
	#parser.add_argument('-d', action="store_true")

	# now, we can access all defined arguments by using args.'name_of_argument_here' e.g. args.dictdir
	args = parser.parse_args()



	target_set = sys.stdin.readlines()

	# TODO: implement the loading in the dict samples

	# loop to load each sample