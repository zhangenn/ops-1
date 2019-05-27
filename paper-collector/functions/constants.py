import os


SQL_USER = os.environ.get('SQL_USER')
SQL_PWD = os.environ.get('SQL_PWD')
SQL_HOST = os.environ.get('SQL_HOST')
SQL_DB = os.environ.get('SQL_DB')
SQL_DB_DEST = os.environ.get('SQL_DB')

START_INDEX = int(os.environ.get('START', '0'))
FETCH_MAX = int(os.environ.get('MAX'))
FETCH_ADDITIONAL = 200

assert SQL_USER, 'SQL_USER is required.'
assert SQL_PWD, 'SQL_PWD is required.'
assert SQL_HOST, 'SQL_HOST is required.'
assert SQL_DB, 'SQL_DB is required.'
