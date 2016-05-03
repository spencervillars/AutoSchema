import os
import sys
import logging
import pymysql
import sys
import shlex
from .as_classifier import AutoSchemaClassifier as asc
from .packages import connection, special
from .datatypes import type_classifier
from .number_sanitize import check_sequential, mean_sd, pdf
import sqlparse
import random
from munkres import Munkres

munk = Munkres()

TYPE_LOOKUP_TABLE = {

'string':'varchar',
'currency':'float',
'zipcode':'varchar',
'integer':'int',
'telephone':'varchar',
'date':'varchar',#TODO: CHANGE THIS TO SUPPORT INNATE MYSQL DATE TYPE
'time':'varchar'#TODO: CHANGE THIS TO SUPPORT INNATE MYSQL TIME TYPE

}

SAMPLE_SIZE       = 50    # USED FOR RANDOM SAMPLINGS
LARGE_SAMPLE_SIZE = 10000 # MAXIMUM TO EVER READ FROM EXISTING COLUMN TO CALCULATE SIMILARITIES TO NEW CANDIDATE INPUT
DICT_FRACTION     = .333  # FRACTION OF DICTIONARY WE NEED TO HIT BEFORE WE RUN WORDNET 

#WHAT? For some reason sqlparse CANT DEAL WITH PERIODS. MEANING, IT CAN'T HANDLE FLOATS.
HACK_MAGIC = "108276394"


DICTIONARIES = {}
PROPER_NAMES = {} #{"name":[],"country":[],"location":[],"plant":[]}


#
# Load the dictionaries:
#   directory path
#       for every file in directory
#           for every line in file
#
dirname = os.path.dirname(os.path.abspath(__file__)) + '/dictionaries/'
for fn in os.listdir(dirname):
    if os.path.isfile(dirname + fn):
        dict = [];
        file = open(dirname + fn)
        for line in file:
            dict.append(line.rstrip())
        if fn in PROPER_NAMES.keys():
            PROPER_NAMES[fn] = dict
        else:
            DICTIONARIES[fn] = dict

#
# CREDIT: http://stackoverflow.com/questions/736043/checking-if-a-string-can-be-converted-to-float-in-python
#
def isfloat(value):
    try:
        float(value)
        return True
    except ValueError:
        return False

def isint(value):
    try:
        int(value)
        return True
    except ValueError:
        return False

def isstring(value):
    return isinstance(value, str)


#
# An AutoSchema classifier object. Imported from as_classifier.
# The argument is the minimum_set_similarity.
#
cl = asc(150)  # TODO: the AutoSchema object should probably initialize its own as_classifier object


class AutoSchema:

    #
    # Input is the parsed input to mycli (e.g. "INSERT INTO x VALUES (...").
    # Extracts the values in parentheses groups, converts values to an array.
    # e.g. "VALUES (x1,x2),(y1,y2)" are converted into twod_array:
    #
    #   x1 x2
    #   y1 y2
    #
    def parse_values(self, parsed):
        twod_array = [];
        par = parsed.token_next_by_instance(0, sqlparse.sql.Parenthesis)
        
        # Read in the text in parentheses and parse it into actual values.
        while par != None:
            # par points to a parenthesis group token
            # print(par, file=sys.stderr)
            
            parser = shlex.shlex(par.token_next(0).value)
            parser.whitespace += ','
            parser.whitespace_split = True
            values = [x.strip("\'\"").replace(HACK_MAGIC,".") for x in list(parser)]
            twod_array.append(values)
            
            par = parsed.token_next_by_instance(parsed.token_index(par)+1, sqlparse.sql.Parenthesis)
        
        twod_array = [list(i) for i in zip(*twod_array)]
        return twod_array

    #
    # TODO: (future work). This function will check if it's necesarry to
    # rearrange the input or not.
    #
    def check_rearranged(self, parsed):
        return True

    def rearrange(self, parsed, sqlexecute):
        table_name = parsed.tokens[4].value
        
        names = []
        types = []
        values = []
        means = []
        deviations = []
        sequential = []
        
        #
        # extract columns names and their datatypes from table
        #
        for table_column in sqlexecute.columns_type(table_name):
            names.append(table_column[0])
            types.append(table_column[1].upper())
            values.append([])
        
        #
        # get a random sample from table
        #
        select_query = "SELECT * FROM " + table_name + " ORDER BY RAND() LIMIT " + str(LARGE_SAMPLE_SIZE)

        #
        # store values from existing table into 'values' array
        #
        with sqlexecute.conn.cursor() as cur:
            cur.execute(select_query)
            for row in cur:
                i = 0;
                for value in row:
                    values[i].append(value)
                    i += 1
        #
        # compute statistical information for each column if it is an INT or FLOAT
        # otherwise, fill in 0 for that columns in the stats arrays
        #
        for i in range(len(names)):
            sqltype = types[i]
            value_array = values[i]
        
            if sqltype != "FLOAT" and sqltype != "INT":
                means.append(0)
                deviations.append(0)
                sequential.append(0)
                continue

            tuple = mean_sd(value_array)
            means.append(tuple[0])
            deviations.append(tuple[1])
            #sequential.append(check_sequential(value_array))
            
        
        par = parsed.token_next_by_instance(0, sqlparse.sql.Parenthesis)
        
        
        # TODO: ADD SUPPORT FOR "INSERT INTO TABLE (COL1, COL2, COL3) VALUES ()"

        #
        # Read in the text in parentheses (i.e. the input) and parse it into 
        # actual values. 'par' points to a parenthesis group token.
        #
        twod_array = [];  # each row is one row from input
        output_array = []
        while par != None:
            parser = shlex.shlex(par.token_next(0).value)
            parser.whitespace += ','
            parser.whitespace_split = True
            array = [x.strip("\'\"").replace(HACK_MAGIC,".") for x in list(parser)]
           
            twod_array.append(array)

            par = parsed.token_next_by_instance(parsed.token_index(par)+1, sqlparse.sql.Parenthesis)

        #
        # for each row of input (x1,x2,...,xn) we determine which column in the
        # table each xi should go
        #
        for row in twod_array:
     
            score_matrix = []
            
            if len(row) != len(names):
                print("Error: Column number mismatch. Not supported (yet).", file=sys.stderr)
            
            #
            # for each xi
            #
            for column in row:
                scores = []
                
                for i in range(len(row)):
                    name = names[i]
                    sqltype = types[i]
                    column_values = values[i]
                    
                    score = 2
                    
                    #
                    # TODO: (future) we assign score for datatype, but we don't consider the format
                    # of numerical vales, whereas we do consider the semantic meaning of 
                    # string values. We will want to consider numerical format in the future.
                    # Currently our system can't tell the difference between zipcode and
                    # phone number, so this will result in annoying rearrangements
                    #

                    # determine the datatype of xi = column, assign a score based on that
                    if type=="INT" and not isint(column) and not isfloat(column):#type mismatch
                        scores.append(score)
                        continue
                    if type=="FLOAT" and not isfloat(column) and not isint(column):#type mismatch
                        scores.append(score)
                        continue
                    if type=="VARCHAR" and (isfloat(column) or isint(column)):
                        scores.append(1.5) # 1.5 is a more desirable score than 2 because munkres minimizes
                        continue
                
                    if type=="FLOAT" or type=="INT":
                        score = pdf(float(column),float(means[i]),float(deviations[i]))
                        if score != score :
                            score = 0
                    else:
                        # not a float or an int. Assume it's a string now?
                        # ....how do we deal with dates?
                        # TODO: DEAL WITH DATES, TELEPHONE NUMBERS, OTHER ODDLY FORMATTED STRINGS.
                        # ^IMPORTANT
                        
                        sample = [ column_values[i] for i in sorted(random.sample(range(len(column_values)), min(len(column_values),SAMPLE_SIZE))) ]
                        score = cl.computeSimilarityOfStringToColumns([sample],column)[0]
                        score = score / 1000

                    scores.append(1 - score)

                score_matrix.append(scores)
    
            # we should now have a square matrix. Let's check this.
            # print(score_matrix,file=sys.stderr)
            indices = munk.compute(score_matrix)

            rearranged_array = [None] * len(row)
            for x,y in indices:
                rearranged_array[y] = row[x]

            output_array.append(rearranged_array)
        
        # now to generate the sql...
        SQL_QUERY = ""
            
        for token in parsed.tokens:
            SQL_QUERY += token.value
            if token.value.upper() == "VALUES":
                break
        
        for array in output_array:
            #print(array,file=sys.stderr)
            sub_string = " ("
            
            for value in array:
                sub_string += "'" + str(value) + "',"
            sub_string = sub_string.rstrip(", ")
            sub_string += "), "
            SQL_QUERY += sub_string
                
        SQL_QUERY = SQL_QUERY.rstrip(", ")
        print(SQL_QUERY,file=sys.stderr)

        return SQL_QUERY

    #
    # This generates a schema. This function will get called when the client
    # attempts to insert into a table that does not exists in the database
    # AND that has no defined schema. 
    #
    def generate_schema(self, parsed):
        '''
        We want to execute a "create table" statement based on the parsed sequel.
        
        Input: An insert statement that contains all values for a table that
            
        '''
        
        table_name = parsed.tokens[4].value # TODO: should generalize this.
        
        #
        # the values from the input converted into array,
        # e.g. the stuff in parantheses in "...VALUES (x1,...,xn),(y1,...,yn)")
        #
        twod_array = self.parse_values(parsed)

        # 
        # We have the contents of the array, now infer the datatype of each column.
        #   1. take a random sample of the input of size SAMPLE_SIZE.
        #   2. test if we can identify a numerical value format in
        #      the sample (e.g. INT, float, zipcode...)
        #   3. otherwise, the sample is a string sample. Try to I.D. it semantically
        #       a. probe the dictionary for direct matches. If samplesize*DICT_FRACTION 
        #          of the sample is found in dictionary, no need to run wordnet
        #       b. else, we do wordnet comparisons on the sample to classify
        #          the input semantically 
        # If it's a numerical value, we run a bunch of regexp tests to see if we
        # infer something about the format of the number. 
        #
        counts = {}
        
        CREATE_TABLE_SQL = "CREATE TABLE "+table_name+" ("

        for column in twod_array: # for each column
            rand_sample = [ column[i] for i in sorted(random.sample(range(len(column)), min(len(column),SAMPLE_SIZE))) ]

            sqltype = type_classifier(rand_sample)
            max_length = 0

            # TODO: we can make this way more DRY
            # ensure columns are uniquely named
            if sqltype not in counts.keys():
                counts[sqltype] = 1
            else:
                counts[sqltype] = counts[sqltype] + 1
        
            label = "Unk"
            
            if sqltype == 'string':
                
                # generate our random samples from dict
                max_length = len(max(column,key=len))
                
                # probe the dictionary with input sample
                max_count = 0
                match = ""
                for key in DICTIONARIES:
                    count = 0
                    for sample in rand_sample:
                        if sample in DICTIONARIES[key]:
                            count = count + 1
                    if max_count < count:
                        max_count = count
                        match = key
                
                # if able to get enough dictionary matches, no need to run wordnet
                if max_count > len(rand_sample)*DICT_FRACTION:
                    label = match
                # else, we run wordnet for sematic similarity between dict/input samples
                else:
                    dict_samples = {}
                    for key in DICTIONARIES:
                        value = DICTIONARIES[key]
                        dict_samples[key] = [ value[i] for i in sorted(random.sample(range(len(value)), min(len(value),SAMPLE_SIZE))) ]
                    
                    max_similarity = 0 	# handles the similarity acceptance threshold on server side?
                    for d in dict_samples:
                        dict_sample = dict_samples[d]
                        print('Using wordnet here')
                        spts = cl.findShortestPathToSet(dict_sample,rand_sample)
                        #print(d + " " + str(spts))
                        if spts > max_similarity and spts >= cl.minimum_set_similarity:
                            max_similarity = spts
                            label = d

                # ensure columns are uniquely named
                if label not in counts.keys():
                    counts[label] = 1
                else:
                    counts[label] = counts[label] + 1
                label = label + str(counts[label])

            else:
                label = sqltype + str(counts[sqltype])

            if sqltype in TYPE_LOOKUP_TABLE.keys():
                sqltype = TYPE_LOOKUP_TABLE[sqltype]

            sqltype = sqltype.upper()

            CREATE_TABLE_SQL += str(label) + " " + sqltype + ("("+str(max_length+100)+")" if sqltype == 'VARCHAR' else "") + ","
            
        # alright, now actually create the table.
        
        CREATE_TABLE_SQL = CREATE_TABLE_SQL.rstrip(",") # remove the last comma
        CREATE_TABLE_SQL += ")"

        return CREATE_TABLE_SQL


			