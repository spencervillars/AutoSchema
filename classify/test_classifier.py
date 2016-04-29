#!/usr/bin/python3

from as_classifier import AutoSchemaClassifier as asc 

# 
# the first argument is minimum acceptable set similarity 
# (set similarity = median shortest path between two sets)
#
# if you want to load the sets from the server, you intialize
# asc like this:
# cl  = asc(.2,"example.com/class_list", 
#			   "example.com/sample/", 
#			   15)
# note that if you intialize like this, you can get the
# classification of a set (which is read in from stdin)
# by calling cl.findClosestSet(), which will return the string
# of the classification


# for testing purposes, i will simply set the min set similarity
# and then pass in my own dictionaries
cl = asc(.2)


classes = ["animal", "location"]

dict_samples = {
				"animal"  :["dog","cat", "mouse"],
				"location":["alabama", "new_haven", "portland"]
				}


input_sample = ["new_york", "nevada", "maryland"]

classification = ""
max_similarity = 0 	# handles the similarity acceptance threshold on server side?
for d in classes:
	dict_sample = dict_samples[d]
	spts = cl.findShortestPathToSet(dict_sample, 
									  input_sample)
	print(d + " " + str(spts))
	if spts > max_similarity and spts >= cl.minimum_set_similarity:
		max_similarity = spts
		classification = d

print("the classification we picked: " + classification)