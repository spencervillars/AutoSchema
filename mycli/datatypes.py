import re
import sys
from collections import Counter

def type_classifier(words):
	'''
	Input: list of strings, ie words
	Process this list using regex expressions to figure out what data type they represent.
	Possible data types:
	1. Date
	2. Time
	3. Currency value
	4. Telephone number
	5. Zipcode
	6. Float
	7. Integer
	8. String
	'''
	results = []
	for word in words:
		
		###### Date
		date1 = re.match(r'[0-9]{2}(\/|-|\.)[0-9]{2}(\/|-|\.)([0-9]{2}){1,2}\b', word)
		date2 = re.match(r'[0-9]{0,2}(th|rd|st)?( )?((J|j)(an(uary)?)|((F|f)(eb(ruary)?))|((M|m)(ar(ch)?))|((A|a)(pr(il)?))|((M|m)(ay))|((J|j)(un(e)?))|((J|j)(ul(y)?))|((A|a)(ug(ust)?))|((S|s)(ep(tember)?))|((O|o)(ct(ober)?))|((N|n)(ov(ember)?))|((D|d)(ec(ember)?)))', word)
		if date1 or date2:
			results.append('date')
			continue

		###### Time
		time1 = re.match(r'[0-9]{2}(:)[0-9]{2}( (hours|hrs))?\b', word)
		time2 = re.match(r'[0-9]?(:)[0-9]{2}(( )?(PM|pm|AM|am))?\b', word)
		if time1 or time2:
			results.append('time')
			continue

		###### Currency
		curr = re.match(r'[0-9]+(,[0-9]{3})*\.[0-9]{2}$', word)
		if curr:
			results.append('currency')
			continue

		###### Telephone
		tele = re.match(r'(\()?[0-9]{3}((\)( )?)?|-)[0-9]{3}( |-)[0-9]{4}\b', word)
		if tele:
			results.append('telephone')
			continue

		###### Zipcode
		zipcode = re.match(r'^\d{5}([-\s]\d{4})?$', word)
		if zipcode:
			results.append('zipcode')
			continue

		###### Float
		fl = re.match(r'^[0-9]*\.[0-9]+$', word)
		if fl:
			results.append('float')
			continue

		###### Integer
		integer = re.match(r'^[0-9]+$', word)
		if integer:
			results.append('integer')
			continue

		###### String
		else:
			results.append('string')
	
	# print(results)
	# print(Counter(results))
	counts = Counter(results)
	total_count = len(words)
	

	# Strings can only be strings, so return this first.
	if counts['string'] > 0:
		return 'string'

	#
	# These are irreconcilable data types. If any of them exist without all of them existing, we return string.
	#
	for a in ['telephone','time','date']:
		if counts[a] > 0:
			return a if counts[a] == total_count else 'string'

	# We now know we have no strings and no weird data types. Classify the remaining ones.
	# float > currency > integer > zipcode
	# zipcode can be an integer can be a currency can be a float

	if counts['float'] > 0:
		return 'float'
	if counts['currency'] > 0:
		return 'currency'
	if counts['integer'] > 0:
		return 'integer'
	if counts['zipcode'] > 0:
		return 'zipcode'

	print("Error: Reached end of classification without returning type.",file=sys.stderr)
	return 'string'#this will never happen....?


