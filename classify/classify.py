#!/usr/bin/python3
import fileinput
import os
import sys

import pickle
from os import walk
from sys import argv, stdin
from textblob.classifiers import NaiveBayesClassifier

from itertools import product

from textblob import Word
from textblob.wordnet import NOUN

import requests
#e.g. requests.get('https://example.com/')

import argparse
parser = argparse.ArgumentParser(description="process args")
parser.add_argument("dictdir")
parser.add_argument('-d', action="store_true")

args = parser.parse_args()

target_set = sys.stdin.readlines()




def loadClassList(url_to_class_list):
	r = requests.get(url_to_class_list)
	if r.code_status == requests.codes.ok:
		return str.splitlines(r.text)
	else:
		return []

def loadSample(url_to_sample, class_name, sample_size):
	r = requests.get( url_to_class_list, params={'n':str(sample_size)} )
	sample = str.splitlines(r.text)
	if r.code_status == requests.codes.ok:
		return str.splitlines(r.text)
	else:
		return []

from statistics import median

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



