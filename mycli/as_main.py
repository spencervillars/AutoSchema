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
'zipcode':'int',
'integer':'int',
'telephone':'varchar',
'date':'varchar',#TODO: CHANGE THIS TO SUPPORT INNATE MYSQL DATE TYPE
'time':'varchar'#TODO: CHANGE THIS TO SUPPORT INNATE MYSQL TIME TYPE

}

SAMPLE_SIZE = 50#USED FOR RANDOM SAMPLINGS
LARGE_SAMPLE_SIZE = 10000#MAXIMUM TO EVER READ FROM EXISTING COLUMN TO CALCULATE SIMILARITIES TO NEW CANDIDATE INPUT

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
    def parse_values(self, parsed):
        twod_array = [];
        par = parsed.token_next_by_instance(0, sqlparse.sql.Parenthesis)
        
        #Read in the text in parentheses and parse it into actual values.
        while par != None:
            #par points to a parenthesis group token
            #print(par, file=sys.stderr)
            
            parser = shlex.shlex(par.token_next(0).value)
            parser.whitespace += ','
            parser.whitespace_split = True
            values = [x.strip("\'\"").replace(HACK_MAGIC,".") for x in list(parser)]
            twod_array.append(values)
            
            par = parsed.token_next_by_instance(parsed.token_index(par)+1, sqlparse.sql.Parenthesis)
        
        twod_array = [list(i) for i in zip(*twod_array)]
        return twod_array

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
            type = types[i]
            value_array = values[i]
        
            if type != "FLOAT" and type != "INT":
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

         
        for row in twod_array:
     
            score_matrix = []
            
            if len(row) != len(names):
                print("Error: Column number mismatch. Not supported (yet).", file=sys.stderr)
            
            #print("testing3",file=sys.stderr)
            for column in row:
                scores = []
                
                #print("testing4",file=sys.stderr)
                for i in range(len(row)):
                    #print("testing5",file=sys.stderr)
                    name = names[i]
                    type = types[i]
                    column_values = values[i]
                    
                    score = 2
                    
                    #print(column,file=sys.stderr)
                    
                    if type=="INT" and not isint(column) and not isfloat(column):#type mismatch
                        scores.append(score)
                        continue
                    if type=="FLOAT" and not isfloat(column) and not isint(column):#type mismatch
                        scores.append(score)
                        continue
                    if type=="VARCHAR" and (isfloat(column) or isint(column)):
                        scores.append(1.5)
                        continue
                
                    if type=="FLOAT" or type=="INT":
                        score = pdf(float(column),float(means[i]),float(deviations[i]))
                        if score != score :
                            score = 0
                    else:
                        #not a float or an int. Assume it's a string now?
                        #....how do we deal with dates?
                        #TODO: DEAL WITH DATES, TELEPHONE NUMBERS, OTHER ODDLY FORMATTED STRINGS.
                        #^IMPORTANT
                        
                        sample = [ column_values[i] for i in sorted(random.sample(range(len(column_values)), min(len(column_values),SAMPLE_SIZE))) ]
                        score = cl.computeSimilarityOfStringToColumns([sample],column)[0]
                        score = score / 1000

                    scores.append(1 - score)

                score_matrix.append(scores)
    
            #vwe should now have a square matrix. Let's check this.
            #vprint(score_matrix,file=sys.stderr)
            indices = munk.compute(score_matrix)

            rearranged_array = [None] * len(row)
            for x,y in indices:
                rearranged_array[y] = row[x]
            #print(rearranged_array,file=sys.stderr)
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

    def generate_schema(self, parsed):
        '''
        We want to execute a "create table" statement based on the parsed sequel.
        
        Input: An insert statement that contains all values for a table that
            
        '''
        
        table_name = parsed.tokens[4].value # TODO: should generalize this.
        
        twod_array = self.parse_values(parsed)

        #We have the contents of the array, now generate our types.
        
        counts = {}
        
        CREATE_TABLE_SQL = "CREATE TABLE "+table_name+" ("

        for column in twod_array:#for each column
            rand_sample = [ column[i] for i in sorted(random.sample(range(len(column)), min(len(column),SAMPLE_SIZE))) ]

            type = type_classifier(rand_sample)
            max_length = 0

            if type not in counts.keys():
                counts[type] = 1
            else:
                counts[type] = counts[type] + 1
        
            label = "Unk"
            
            if type == 'string':
                
                #generate our random samples from dict
                max_length = len(max(column,key=len))
                
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

                if max_count > len(rand_sample)/3:
                    label = match
                else:
                    dict_samples = {}
                    for key in DICTIONARIES:
                        value = DICTIONARIES[key]
                        dict_samples[key] = [ value[i] for i in sorted(random.sample(range(len(value)), min(len(value),SAMPLE_SIZE))) ]
                    
                    max_similarity = 0 	# handles the similarity acceptance threshold on server side?
                    for d in dict_samples:
                        dict_sample = dict_samples[d]
                        
                        spts = cl.findShortestPathToSet(dict_sample,rand_sample)
                        #print(d + " " + str(spts))
                        if spts > max_similarity and spts >= cl.minimum_set_similarity:
                            max_similarity = spts
                            label = d

            else:
                label = type + str(counts[type])

            if type in TYPE_LOOKUP_TABLE.keys():
                type = TYPE_LOOKUP_TABLE[type]

            type = type.upper()

            CREATE_TABLE_SQL += str(label) + " " + type + ("("+str(max_length+100)+")" if type == 'VARCHAR' else "") + ","
            
        #alright, now actually create the table.
        
        CREATE_TABLE_SQL = CREATE_TABLE_SQL.rstrip(",")#remove the last comma
        CREATE_TABLE_SQL += ")"

        return CREATE_TABLE_SQL


			