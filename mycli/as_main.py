import os
import sys
import logging
import pymysql
import sys
import shlex
from .as_classifier import AutoSchemaClassifier as asc
from .packages import connection, special
from .datatypes import type_classifier
import sqlparse
import random

TYPE_LOOKUP_TABLE = {

'string':'varchar',
'currency':'float',
'zipcode':'int',
'integer':'int',
'telephone':'varchar',
'date':'varchar',#TODO: CHANGE THIS TO SUPPORT INNATE MYSQL DATE TYPE
'time':'varchar'#TODO: CHANGE THIS TO SUPPORT INNATE MYSQL TIME TYPE

}

SAMPLE_SIZE = 100#USED FOR RANDOM SAMPLINGS


#WHAT? For some reason sqlparse CANT DEAL WITH PERIODS. MEANING, IT CAN'T HANDLE FLOATS.
HACK_MAGIC = "108276394"


DICTIONARIES = {}
PROPER_NAMES = {}#{"name":[],"country":[],"location":[],"plant":[]}

#directory path
#for every file in directory

#for every line in file
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

    def rearrange(self, parsed):
        table_name = parsed.tokens[4].value
        
        names = []
        types = []
        values = []
        
        for table_column in self.columns_type(table_name):
            names.append(table_name[0])
            types.append(table_name[1])
            values.append([])
            
        select_query = "SELECT * FROM " + table_name

        with self.conn.cursor() as cur:
            cur.execute(select_query)
            for row in cur:
                i = 0;
                for value in row:
                    values[i].append(value)
                    i += 1
        

    def generate_schema(self, parsed):
        '''
        We want to execute a "create table" statement based on the parsed sequel.
        
        Input: An insert statement that contains all values for a table that
            
        '''
        #print(stmt.tokens, file=sys.stderr)
        
        table_name = parsed.tokens[4].value#TODO: should generalize this.
        
        twod_array = self.parse_values(parsed)

        #We have the contents of the array, now generate our types.
        
        cl = asc(.2)
        
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

        #TODO: do this in the outside!!!!!!!!


        #print(CREATE_TABLE_SQL, file=sys.stderr)
        self.execute_normal_sql(CREATE_TABLE_SQL)

			