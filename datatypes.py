import re
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
		time1 = re.match(r'[0-9]{2}(:|\.)[0-9]{2}( (hours|hrs))?\b', word)
		time2 = re.match(r'[0-9]?(:|\.)[0-9]{2}(( )?(PM|pm|AM|am))?\b', word)
		if time1 or time2:
			results.append('time')
			continue

		###### Currency
		curr = re.match(r'\$?[0-9]+(,[0-9]{3})*\.[0-9]{2}$', word)
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
	return(Counter(results).most_common()[0][0])














