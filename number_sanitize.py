import scipy.stats as stats
import numpy as np

def check_sequential(array):
	'''
	Input: list of numbers
	Output: True if the numbers do not correspond to a normal distribution with p < 0.05
	False otherwise
	'''
	zval, pval = stats.normaltest(array)
	if pval < 0.05:
		return True
	else:
		return False

def mean_sd(array):
	'''
	Input: list of numbers
	Output: (mean, sd)
	'''
	return (np.mean(array), np.std(array))

def pdf(input, mean, sd):
	'''
	Input: new value, mean, sd
	Output: pdf that new value belongs to this list of numbers assuming the list is
	modelled as a normal distribution
	'''
	return (stats.norm.pdf(input, loc=mean, scale=sd))
