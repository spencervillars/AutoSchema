import logging
import pymysql
import sys
import shlex
import os
import random
from .packages import connection, special
from .as_main import AutoSchema 
import sqlparse

_logger = logging.getLogger(__name__)

#WHAT? For some reason sqlparse CANT DEAL WITH PERIODS. MEANING, IT CAN'T HANDLE FLOATS.
HACK_MAGIC = "108276394"

autoschema = AutoSchema()

class SQLExecute(object):

    databases_query = '''SHOW DATABASES'''

    tables_query = '''SHOW TABLES'''

    version_query = '''SELECT @@VERSION'''

    version_comment_query = '''SELECT @@VERSION_COMMENT'''

    show_candidates_query = '''SELECT name from mysql.help_topic WHERE name like "SHOW %"'''

    users_query = '''SELECT CONCAT("'", user, "'@'",host,"'") FROM mysql.user'''

    functions_query = '''SELECT ROUTINE_NAME FROM INFORMATION_SCHEMA.ROUTINES
    WHERE ROUTINE_TYPE="FUNCTION" AND ROUTINE_SCHEMA = "%s"'''

    table_columns_query = '''select TABLE_NAME, COLUMN_NAME from information_schema.columns
                                    where table_schema = '%s'
                                    order by table_name,ordinal_position'''
    
    columns_info_query = '''select COLUMN_NAME,DATA_TYPE from information_schema.columns
                                            where table_schema = '%s' AND table_name = '%s'
                                            order by ordinal_position'''

    def __init__(self, database, user, password, host, port, socket, charset,
                 local_infile, ssl=False):
        self.dbname = database
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.socket = socket
        self.charset = charset
        self.local_infile = local_infile
        self.ssl = ssl
        self._server_type = None
        self.connect()

    def connect(self, database=None, user=None, password=None, host=None,
            port=None, socket=None, charset=None, local_infile=None, ssl=None):
        db = (database or self.dbname)
        user = (user or self.user)
        password = (password or self.password)
        host = (host or self.host)
        port = (port or self.port)
        socket = (socket or self.socket)
        charset = (charset or self.charset)
        local_infile = (local_infile or self.local_infile)
        ssl = (ssl or self.ssl)
        _logger.debug('Connection DB Params: \n'
            '\tdatabase: %r'
            '\tuser: %r'
            '\thost: %r'
            '\tport: %r'
            '\tsocket: %r'
            '\tcharset: %r'
            '\tlocal_infile: %r'
            '\tssl: %r',
            database, user, host, port, socket, charset, local_infile, ssl)

        conn = connection.connect(database=db, user=user, password=password,
                host=host, port=port, unix_socket=socket,
                use_unicode=True, charset=charset, autocommit=True,
                client_flag=pymysql.constants.CLIENT.INTERACTIVE,
                cursorclass=connection.Cursor, local_infile=local_infile,
                ssl=ssl)
        if hasattr(self, 'conn'):
            self.conn.close()
        self.conn = conn
        # Update them after the connection is made to ensure that it was a
        # successful connection.
        self.dbname = db
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.socket = socket
        self.charset = charset
        self.ssl = ssl

    def run(self, statement):
        """Execute the sql in the database and return the results. The results
        are a list of tuples. Each tuple has 4 values
        (title, rows, headers, status).
        """

        # Remove spaces and EOL
        statement = statement.strip()
        if not statement:  # Empty string
            yield (None, None, None, None)

        # Split the sql into separate queries and run each one.
        # Unless it's saving a favorite query, in which case we
        # want to save them all together.
        if statement.startswith('\\fs'):
            components = [statement]

        else:
            components = sqlparse.split(statement)

        for sql in components:
            # Remove spaces, eol and semi-colons.
            sql = sql.rstrip(';')

            # \G is treated specially since we have to set the expanded output.
            if sql.endswith('\\G'):
                special.set_expanded_output(True)
                sql = sql[:-2].strip()
            try:   # Special command
                _logger.debug('Trying a dbspecial command. sql: %r', sql)
                cur = self.conn.cursor()
                for result in special.execute(cur, sql):
                    yield result
            except special.CommandNotFound:  # Regular SQL
                yield self.execute_normal_sql(sql)

    def generate_schema(self, parsed):
        '''
        We want to execute a "create table" statement based on the parsed sequel.
        
        Input: An insert statement that contains all values for a table that
            
        '''
        CREATE_TABLE_SQL =  autoschema.generate_schema(parsed)
        self.execute_normal_sql(CREATE_TABLE_SQL)

    def execute_normal_sql(self, split_sql):
        _logger.debug('Regular sql statement. sql: %r', split_sql)
        
        # 'WHAT? For some reason sqlparse CANT DEAL WITH PERIODS. 
        # MEANING, IT CAN'T HANDLE FLOATS.' -S. Villars, 2016
        split_sql = split_sql.replace(".",HACK_MAGIC);

        #
        # parse the sql input.
        # e.g. stmt => 'INSERT INTO eg_table VALUES (....)'
        #
        parsed = sqlparse.parse(split_sql) 
        stmt = parsed[0] 
        type = stmt.get_type().upper()
        
        #
        # check to see if we are inserting into tablet that already exists.
        # if not, generate its schema.
        # if yes, check to see if the data we need to rearrange input
        #
        if (type == "INSERT" or type == "REPLACE") and str(stmt.tokens[2]).upper() == "INTO":
            # TODO: (in the future) we may want to support some other types 
            # of statments, not just ones the look like "INSERT/REPLACE INTO"
            matched = False
            table_name = str(stmt.tokens[4]).upper()
            for table in self.tables():
                if table_name == table[0].upper():
                    matched = True
                    break
            if not matched:
                # We now have a table that is not already in our database.
                # Let's call our table creation function.
                self.generate_schema(stmt)

            else:
                # TODO: check_rearranged() always returns True
                if autoschema.check_rearranged(0):
                    split_sql = autoschema.rearrange(stmt,self)
    
        split_sql = split_sql.replace(HACK_MAGIC,".");
        cur = self.conn.cursor()
        num_rows = cur.execute(split_sql)
        title = headers = None

        # cur.description is not None for queries that return result sets, e.g.
        # SELECT or SHOW.
        if cur.description is not None:
            headers = [x[0] for x in cur.description]
            status = '{0} row{1} in set'
        else:
            _logger.debug('No rows in result.')
            status = 'Query OK, {0} row{1} affected'
        status = status.format(num_rows, '' if num_rows == 1 else 's')

        return (title, cur if cur.description else None, headers, status)

    def tables(self):
        """Yields table names"""

        with self.conn.cursor() as cur:
            _logger.debug('Tables Query. sql: %r', self.tables_query)
            cur.execute(self.tables_query)
            for row in cur:
                yield row

    def table_columns(self):
        """Yields column names"""
        with self.conn.cursor() as cur:
            _logger.debug('Columns Query. sql: %r', self.table_columns_query)
            cur.execute(self.table_columns_query % self.dbname)
            for row in cur:
                yield row

    def columns_type(self,table):
        """column invormation for a given table"""
        with self.conn.cursor() as cur:
            #_logger.debug('Columns Query. sql: %r', self.table_columns_query)
            cur.execute(self.columns_info_query % (self.dbname,table))
            for row in cur:
                yield row

    def databases(self):
        with self.conn.cursor() as cur:
            _logger.debug('Databases Query. sql: %r', self.databases_query)
            cur.execute(self.databases_query)
            return [x[0] for x in cur.fetchall()]

    def functions(self):
        """Yields tuples of (schema_name, function_name)"""

        with self.conn.cursor() as cur:
            _logger.debug('Functions Query. sql: %r', self.functions_query)
            cur.execute(self.functions_query % self.dbname)
            for row in cur:
                yield row

    def show_candidates(self):
        with self.conn.cursor() as cur:
            _logger.debug('Show Query. sql: %r', self.show_candidates_query)
            try:
                cur.execute(self.show_candidates_query)
            except pymysql.OperationalError as e:
                _logger.error('No show completions due to %r', e)
                yield ''
            else:
                for row in cur:
                    yield (row[0].split(None, 1)[-1], )

    def users(self):
        with self.conn.cursor() as cur:
            _logger.debug('Users Query. sql: %r', self.users_query)
            try:
                cur.execute(self.users_query)
            except pymysql.OperationalError as e:
                _logger.error('No user completions due to %r', e)
                yield ''
            else:
                for row in cur:
                    yield row

    def server_type(self):
        if self._server_type:
            return self._server_type
        with self.conn.cursor() as cur:
            _logger.debug('Version Query. sql: %r', self.version_query)
            cur.execute(self.version_query)
            version = cur.fetchone()[0]
            _logger.debug('Version Comment. sql: %r', self.version_comment_query)
            cur.execute(self.version_comment_query)
            version_comment = cur.fetchone()[0].lower()

        if 'mariadb' in version_comment:
            product_type = 'mariadb'
        elif 'percona' in version_comment:
            product_type = 'percona'
        else:
            product_type = 'mysql'

        self._server_type = (product_type, version)
        return self._server_type
