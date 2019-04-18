import pymysql.cursors
import json
import aeneid.utils.utils as ut
import aeneid.utils.dffutils as db
import aeneid.dbservices.DataExceptions
from aeneid.dbservices.RDBDataTable import RDBDataTable
import pymysql


db_schema = None                                # Schema containing accessed data
cnx = None                                      # DB connection to use for accessing the data.
key_delimiter = '_'                             # This should probably be a config option.

# Is a dictionary of {table_name : [primary_key_field_1, primary_key_field_2, ...]
# Used to convert a list of column values into a template of the form { col: value }
primary_keys = {}

# This dictionary contains columns mappings for nevigating from a source table to a destination table.
# The keys is of the form sourcename_destinationname. The entry is a list of the form
# [[sourcecolumn1, destinationcolumn1], ...
join_columns = {}

# Data structure contains RI constraints. The format is a dictionary with an entry for each schema.
# Within the schema entry, there is a dictionary containing the constraint name, source and target tables
# and key mappings.
ri_constraints = None

data_tables = {}


# TODO This is a bit of a hack and we should clean up.
# We should load information from database or configuration file.
# people = RDBDataTable("lahman2017.people", key_columns=['playerID'])
# data_tables["lahman2017.people"] = people
# batting = RDBDataTable("lahman2017.batting", key_columns=['playerID', 'yearID', 'teamID', 'stint'])
# data_tables["lahman2017.batting"] = batting
# appearances = RDBDataTable("lahman2017.appearances", key_columns=['playerID', 'yearID', 'teamID'])
# data_tables["lahman2017.appearances"] = appearances
# offices = RDBDataTable("classiccars.offices", key_columns=['officeCode'])
# data_tables["classiccars.offices"] = offices
# test = RDBDataTable("lahman2017.test", key_columns=['id'])
# data_tables["lahman2017.test"] = test

cnx = pymysql.connect(
    host="localhost",
    database="lahman2017",
    user="dbuser",
    password="dbuser",
    charset = 'utf8mb4',
    cursorclass = pymysql.cursors.DictCursor)

def get_data_table(table_name):

    result = data_tables.get(table_name, None)
    if result is None:
        result = RDBDataTable(table_name)
        data_tables[table_name] = result

    return result

def get_by_template(table_name, template, field_list=None, limit=None, offset=None, order_by=None, commit=True):

    dt = get_data_table(table_name)
    result = dt.find_by_template(template, field_list, limit, offset, order_by, commit)
    return result.get_rows()

def get_by_primary_key(table_name, key_fields, field_list=None, commit=True):

    dt = get_data_table(table_name)
    result = dt.find_by_primary_key(key_fields, field_list)
    return result


def get_join_column_mapping(schema1, table1, schema2, table2):
    q = """
        SELECT
          TABLE_NAME,
          COLUMN_NAME,
          CONSTRAINT_NAME,
          REFERENCED_TABLE_NAME,
          REFERENCED_COLUMN_NAME
        FROM
          INFORMATION_SCHEMA.KEY_COLUMN_USAGE
        WHERE
         (REFERENCED_TABLE_SCHEMA = %s AND REFERENCED_TABLE_NAME = %s
           AND TABLE_SCHEMA = %s AND TABLE_NAME = %s)
         OR
         (REFERENCED_TABLE_SCHEMA = %s AND REFERENCED_TABLE_NAME = %s
           AND TABLE_SCHEMA = %s AND TABLE_NAME = %s)

    """

    params = (schema1, table1, schema2, table2, schema2, table2, schema1, table1)

    cursor = cnx.cursor()
    final_q = cursor.mogrify(q, params)
    # print(final_q)
    r = cursor.execute(q, params)
    constraints = cursor.fetchall()

    result = {}

    for c in constraints:

        n = c['CONSTRAINT_NAME']
        e = result.get(n, None)

        if e is None:
            e = {}
            e['CONSTRAINT_NAME'] = n
            e['MAP'] = []
            result[n] = e

        this_m = {k: c[k] for k in ['TABLE_NAME', 'COLUMN_NAME',
                                    'REFERENCED_TABLE_NAME', 'REFERENCED_COLUMN_NAME']}
        e['MAP'].append(this_m)

    return result

def get_by_join(table_name, children, template, field_list=None, limit=None, offset=None, order_by=None, commit=True):

    dt = get_data_table(table_name)
    cons = {}
    dbname, referenced_table = table_name.split(".")[0], table_name.split(".")[1]
    cons[referenced_table.lower()] = dt._get_primary_key_columns()[0].lower()

    for child in children:
        paths = get_join_column_mapping(dbname, referenced_table, dbname, child)
        paths = paths[list(paths.keys())[0]]["MAP"][0]
        name = paths["TABLE_NAME"]
        column = paths["COLUMN_NAME"]
        cons[name.lower()] = column.lower()

    result = dt.join_by_template(template, children, field_list, limit, offset, order_by, commit, cons)
    return result.get_rows()


def create(table_name, new_value):
    dt = get_data_table(table_name)
    result = dt.insert(new_value)
    return result

def delete(table_name, key_columns):
    dt = get_data_table(table_name)
    result = dt.delete_by_key(key_columns)
    return result

def update(table_name, key_columns, new_values):
    dt = get_data_table(table_name)
    result = dt.update_by_key(key_columns, new_values)
    return result
















