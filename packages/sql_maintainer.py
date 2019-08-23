
import mysql.connector
import csv
from mysql.connector import Error
from tqdm import tqdm
from .helper_functions import chunks
from .helper_functions import get_sql_column_names
from dateutil.parser import parse


MAWP_60_DATEMAP = [0, 20]


# Convert a csv file to sequence of insert sql queries.
def csv_to_sql_insert_queries(csv_contents, connection, table_name, max_rows=25):
    columns = list(get_sql_column_names())
    chunked_text_list = list(chunks(csv_contents, max_rows))
    for each_chunk in tqdm(iterable=chunked_text_list, total=len(list(chunked_text_list)), desc="Building insert commands."):
        cursor, result = submit_query(convert_list_to_sql_insert(each_chunk, table_name, columns), connection)
    return cursor, result


# Convert a list to an insert sql query.
# Note: With SQL, the data might not have to be sorted by station? All station data just has to be chronological?
def convert_list_to_sql_insert(chunk_list, table_name, columns, destination='VALUES'):
    insert_string = "INSERT INTO %s%s\n%s" % (table_name, list_to_parenthesis_block(columns), destination)
    index = 0
    list_size = len(chunk_list)
    num_columns = len(columns)
    for each_line in chunk_list:
        nullified_line = ['NULL' if x == '' else x for x in each_line]
        insert_string += list_to_parenthesis_block(extend_list_to_count(num_columns, nullified_line), MAWP_60_DATEMAP)
        if index != list_size - 1:
            insert_string += ','
            insert_string += '\n'
        index += 1
    insert_string += ';'
    return insert_string


# Returns a string in the format: (item1, item2, ...., itemnN).
def list_to_parenthesis_block(contents, date_map="NONE"):
    if date_map != "NONE":
        for each_index in date_map:
            contents[each_index] = '\'' + contents[each_index] + '\''
    return "(" + ",".join(map(str, contents)) + ")"


def is_date(string, fuzzy=False):
    """
    Return whether the string can be interpreted as a date.

    :param string: str, string to check for date
    :param fuzzy: bool, ignore unknown tokens in string if True
    """
    try:
        parse(string, fuzzy=fuzzy)
        return True

    except ValueError:
        return False


def extend_list_to_count(num_columns, contents):
    num_items = len(contents)
    if len(contents) < num_columns:
        for each in range(num_columns - num_items):
            contents.append('NULL')
    return contents


def nullify_insert_sql_string(string):
    return string.replace('(,', '(NULL,').replace(',,', ',NULL,').replace(',)', ',NULL)')


def open_connection(user_host='localhost', database='mbag', user_name='LocalAdmin', password='mawp209MAWP@)('):
    try:
        connection = mysql.connector.connect(host=user_host, database=database, user=user_name, password=password)
        print("Successfully opened database.")
    except Error as error:
        print('Failed to connect to database'.format(error))
    return connection


def submit_query(query, connection):
    try:
        cursor = connection.cursor()
        result = cursor.execute(query)
        print('Query completed successfully.')
    except Error as error:
        print('Failed to create table.')
    return cursor, result


def close_connection(connection, cursor):
    if connection.is_connected():
        cursor.close()
        connection.close()
        print("MySQL connection is closed.")