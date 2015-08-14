

from boto.s3.connection import S3Connection
from bson.objectid import ObjectId
from pymongo import MongoClient
from json import dumps, loads
from datetime import datetime
import re

import cromo_aws
import cromo_database
import cromo_templates

import cromo_email

# this conversion isn't normal.
SOURCE_DB_NAME = {'campaigns' : cromo_database.CROMO_CAMPAIGNS_DB,
                  'perks'     : cromo_database.CROMO_PERK_DB,
                  'posts'     : cromo_database.CROMO_POST_DB,
                  'links'     : cromo_database.CROMO_LINKS_DB,
                  'updates'   : cromo_database.CROMO_CAMPAIGN_UPDATES_DB,
                  'users'     : cromo_database.CROMO_USER_DB,
                  'beta'      : cromo_database.CROMO_BETA_DB,
                  'commits'   : cromo_database.CROMO_COMMIT_DB}

def tokenize(clauses):
    tokens = []
    in_quotes = False

    for piece in clauses.split(' '):
        if in_quotes:
            # if we're in quotes now, just tag on this string.
            tokens[len(tokens)-1] += ' ' + piece

            # does this piece end with ' but not \'
            if piece.endswith("'") and len(piece) > 1:
                if piece[len(piece)-2] != '\\':
                    in_quotes = False
        else:
            tokens.append(piece)
            if piece.startswith("'"):
                in_quotes = True

    return tokens

def process_obj(value_str):
    """obj(asdfasdfasdf) -> ObjectId('....')"""

    obj_m = re.match('obj\((.+)\)', value_str)
    if obj_m:
        return ObjectId(obj_m.group(1))

def process_date(value_str):
    """date(2015-05-06) -> datetime(2015, 05, 06)"""

    date_str = value_str[5:len(value_str)-1]
    return datetime.strptime(date_str, "%Y-%m-%d")

def process_where(fieldname, comparison_type, value_str):
    """
    field like string (string)
    field = True/False (boolean)
    field has string (string in array)
    field >, >=, <, <=, = (int)
    field >, >=, <, <=, = date(YYYY-MM-DD)
    field is empty (array)
    field not empty (array)
    """

    if comparison_type == 'like':
        # boring string, remove quotes
        if value_str[0] == '\'' or value_str[0] == '"':
            new_string = value_str[1:len(value_str)-1]
        else:
            new_string = value_str
        query_dict = {fieldname : {'$regex' : new_string, '$options' : 'i'}}
    elif comparison_type == 'is':
        if value_str == 'empty':
            # array
            query_dict = {fieldname : {'$size' : 0}}
    elif comparison_type == 'not':
        query_dict = {fieldname : {'$ne' : []}}
    elif comparison_type == '=':
        # integer, we don't do decimal.
        if value_str.startswith('date('):
            query_dict = {fieldname : {'$eq' : process_date(value_str)}}
        elif value_str.startswith('obj('):
            query_dict = {fieldname : {'$eq' : process_obj(value_str)}}
        # boolean
        elif value_str == 'true':
            query_dict = {fieldname : True}
        elif value_str == 'false':
            query_dict = {fieldname : False}
        else:
            query_dict = {fieldname : int(value_str)}
    elif comparison_type == '<':
        if value_str.startswith('date('):
            query_dict = {fieldname : {'$lt' : process_date(value_str)}}
        else:
            query_dict = {fieldname : {'$lt' : int(value_str)}}
    elif comparison_type == '<=':
        if value_str.startswith('date('):
            query_dict = {fieldname : {'$lte' : process_date(value_str)}}
        else:
            query_dict = {fieldname : {'$lte' : int(value_str)}}
    elif comparison_type == '>':
        if value_str.startswith('date('):
            query_dict = {fieldname : {'$gt' : process_date(value_str)}}
        else:
            query_dict = {fieldname : {'$gt' : int(value_str)}}
    elif comparison_type == '>=':
        if value_str.startswith('date('):
            query_dict = {fieldname : {'$gte' : process_date(value_str)}}
        else:
            query_dict = {fieldname : {'$gte' : int(value_str)}}
    elif comparison_type == 'has':
        if value_str[0] == '\'' or value_str[0] == '"':
            new_string = value_str[1:len(value_str)-1]
        else:
            new_string = value_str
        query_dict = {fieldname : new_string}
    else:
        query_dict = {}
    
    return query_dict

def array_from_dict(field, operation):
    
    try:
        return [d[operation['key']] for d in field]
    except:
        return []

def sum_after_key(field, operation):
    
    try:
        return sum(field[operation['key']])
    except:
        return 0

def sum_over_key(field, operation):
    
    try:
        return sum([d[operation['key']] for d in field])
    except:
        return 0

def len_after_key(field, operation):
    """."""

    if operation['key'] in field:
        result = field[operation['key']]
        return len(result)
    
    return 0

def last_after_key(field, operation):
    """."""

    if operation['key'] in field:
        result = field[operation['key']]
        return last_in_array(result)

    return None

def key_after_last(field, operation):
    """Get the last, then key off it."""
    
    result = last_in_array(field)
    if result is None:
        return None

    if operation['key'] in result:
        return result[operation['key']]

    return None

def last_in_array(array, operation = None):
    """Return the last element in an array.
    
    I think this is just [-1]
    
    """

    if len(array) == 0:
        return None

    return array[-1]

def sum_of_array(array, operation = None):
    
    try:
        return sum(array)
    except:
        return 0

def len_of_thing(thing, operation = None):
    """Wrapped to be consistent and because I may provide more information 
    in case I try to do this recursively."""

    try:
        return len(thing)
    except:
        return 0

def key_in_dict(dt, operation):
    
    if operation['key'] in dt:
        return dt[operation['key']]
    
    return None

def process_fieldlist(field_list_str):
    """Check for fields with built-in operations."""

    field_list = []
    field_list_special = []

    for field_name in field_list_str.split(','):
        f = field_name.strip()
        # could make this a list, but then some of the other bits are annoying.
        len_op = re.match('len\((.+)\)', f)
        last_op = re.match('last\((.+)\)', f)
        sum_op = re.match('sum\((.+)\)', f)
        dict_in_array_op = re.match('(.+)\[\*\]\[(.+)\]', f)
        dict_op = re.match('(.+)\[(.+)\]', f)
        key_after_last_op = re.match('last\((.+)\)\[(.+)\]', f)
        last_after_key_op = re.match('last\((.+)\[(.+)\]\)', f)
        len_after_key_op = re.match('len\((.+)\[(.+)\]\)', f)
        sum_over_keys_op = re.match('sum\((.+)\[\*\]\[(.+)\]\)', f)
        sum_after_key_op = re.match('sum\((.+)\[(.+)\]\)', f)

        # the way I do this is by function pointer, which isn't going to work
        # for every type...  so i'll need basically two types or something even
        # more generic that often time throws out an "extra" parameter.
        # XXX: Can do this recursively.
        
        # The order on this list is important.
        if key_after_last_op:
            field_list_special.append({'outer' : key_after_last_op.group(1),
                                       'name'  : f,
                                       'key'   : key_after_last_op.group(2), # could be nice and recursive
                                       'op'    : key_after_last})
        elif last_after_key_op:
            field_list_special.append({'outer' : last_after_key_op.group(1),
                                       'name'  : f,
                                       'key'   : last_after_key_op.group(2),
                                       'op'    : last_after_key})
        elif len_after_key_op:
            field_list_special.append({'outer' : len_after_key_op.group(1),
                                       'name'  : f,
                                       'key'   : len_after_key_op.group(2),
                                       'op'    : len_after_key})
        elif sum_over_keys_op:
            field_list_special.append({'outer' : sum_over_keys_op.group(1),
                                       'name'  : f,
                                       'key'   : sum_over_keys_op.group(2),
                                       'op'    : sum_over_key})
        elif sum_after_key_op:
            field_list_special.append({'outer' : sum_after_key_op.group(1),
                                       'name'  : f,
                                       'key'   : sum_after_key_op.group(2),
                                       'op'    : sum_after_key})
        elif dict_in_array_op:
            field_list_special.append({'outer' : dict_in_array_op.group(1),
                                       'name'  : f,
                                       'key'   : dict_in_array_op.group(2),
                                       'op'    : array_from_dict})
        elif dict_op:
            field_list_special.append({'outer' : dict_op.group(1),
                                       'name'  : f,
                                       'key'   : dict_op.group(2),
                                       'op'    : key_in_dict})
        elif len_op:
            field_list_special.append({'outer' : len_op.group(1), 
                                       'name'  : f,
                                       'op'    : len_of_thing})
        elif last_op:
            field_list_special.append({'outer' : last_op.group(1),
                                       'name'  : f,
                                       'op'    : last_in_array})
        elif sum_op:
            field_list_special.append({'outer' : sum_op.group(1),
                                       'name'  : f,
                                       'op'    : sum_of_array})
        else:
            field_list.append(f)

    return field_list, field_list_special

class Sql2Mongo(object):
    
    def __init__(self, query_string):
        self.query_string = query_string
        self.all_keys = {}

        sorted_results = False
        query_dict = {}

        # expecting:
        # select fields from tables where fields order by field
        # select fields from tables where fields
        # select fields from tables order by field
        # select fields from tables

        # select fields [0]
        # tables where .... [1]
        main_pieces = query_string.split('from')
        
        # at a minimum it has select and from
        if 'where' in main_pieces[1]:
            # build query.
            
            main_selectors = main_pieces[1].split('where')
            table = main_selectors[0].strip() # before ther where after the from
            rest = main_selectors[1].strip() # after the where
            
            if 'order by' in main_selectors[1]:
                where_order = main_selectors[1].split('order by')
                
                # this is the list of where clauses
                selectors = where_order[0].strip() # left of 'order by'
                
                # this is the 'order by'
                order_by_piece = where_order[1].strip()
                
                sorted_results = True

                sorted_pieces = order_by_piece.strip().split(' ')
                sorted_field = sorted_pieces[0].strip()
                sorted_dir = 1
                if len(sorted_pieces) == 2:
                    if sorted_pieces[1].strip() == 'asc':
                        sorted_dir = 1
                    elif sorted_pieces[1].strip() == 'desc':
                        sorted_dir = -1

                search_pieces = tokenize(selectors)
            else:
                search_pieces = tokenize(rest)

            # Handle the where clause(s), but only single word strings.
            if len(search_pieces) == 3:
                # field thing value
                fieldname = search_pieces[0]
                comparison_type = search_pieces[1]
                value_str = search_pieces[2]
                query_dict = process_where(fieldname, comparison_type, value_str)
            elif len(search_pieces) == 7:
                fieldname = search_pieces[0]
                comparison_type = search_pieces[1]
                value_str = search_pieces[2]
                query_dict1 = process_where(fieldname, comparison_type, value_str)
            
                fieldname = search_pieces[4]
                comparison_type = search_pieces[5]
                value_str = search_pieces[6]
                query_dict2 = process_where(fieldname, comparison_type, value_str)

                if search_pieces[3] == 'and':
                    query_dict = {'$and' : [query_dict1, query_dict2]}
                elif search_pieces[3] == 'or':
                    query_dict = {'$or' : [query_dict1, query_dict2]}
        else:
            # no where clause, pull the table? then check for sort
            the_pieces = main_pieces[1].strip().split(' ')
            table = the_pieces[0].strip()

        if 'order by' in main_pieces[1]:
            order_main = main_pieces[1].strip().split('order by')
            
            order_by_pieces = order_main[1].strip() # after 'order by'
            
            sorted_results = True

            sorted_pieces = order_by_pieces.strip().split(' ')
            sorted_field = sorted_pieces[0].strip()
            sorted_dir = 1
            if len(sorted_pieces) == 2:
                if sorted_pieces[1].strip() == 'asc':
                    sorted_dir = 1
                elif sorted_pieces[1].strip() == 'desc':
                    sorted_dir = -1

        # 5. Convert the results into rows given the [fields]
        field_list_str = main_pieces[0].split('select')[1].strip()
        if field_list_str == '*':
            field_list = ['*'] # just for debug
            field_list_spec = []
            all_fields = True
        else:
            field_list, field_list_spec = process_fieldlist(field_list_str)
            all_fields = False

        self.valid = True
        if table not in SOURCE_DB_NAME:
            self.valid = False
        real_source = SOURCE_DB_NAME[table]

        self.real_source = real_source
        self.query_dict = query_dict
        self.sorted_results = sorted_results
        self.all_fields = all_fields
        self.field_list = field_list
        self.field_list_spec = field_list_spec

        self.sorted_field = ''
        self.sorted_dir = 1
        if sorted_results:
            self.sorted_field = sorted_field
            self.sorted_dir = sorted_dir
 
        #,
        #
        #x = {'query_dict'      : query_dict,
        #     'field_list'      : field_list,
        #     'field_list_spec' : [],
        #     'real_source'     : real_source,
        #     'sorted_results'  : sorted_results}
        #for f in field_list_spec:
        #    x['field_list_spec'].append(f['name'])
        #if sorted_results:
        #    x['sorted'] = {'sorted_field' : sorted_field,
        #                   'sorted_dir'   : sorted_dir}

        self.x = {'query_string' : query_string}

    def getx(self):
        return self.x

    def process_row(self, row):
        """Process the stupid row."""

        new_row = {}

        if self.all_fields:
            for k,v in row.items():
                if not isinstance(v, basestring):
                    new_row[k] = str(v)
                else:
                    new_row[k] = v

                try:
                    self.all_keys[k] += 1
                except KeyError:
                    self.all_keys[k] = 1
        else:
            for field in self.field_list:
                if field in row:
                    if not isinstance(row[field], basestring):
                        new_row[field] = str(row[field])
                    else:
                        new_row[field] = row[field]
                else:
                    new_row[field] = 'undefined'
            
            # Need to do this recursively to handle multiple things.
            for field in self.field_list_spec:
                if field['outer'] in row:
                    value = field['op'](row[field['outer']], field)
                    if not isinstance(value, basestring):
                        new_row[field['name']] = str(value)
                    else:
                        new_row[field['name']] = value

        # Don't share password hashes, since we store them in a database.
        if self.real_source == cromo_database.CROMO_USER_DB:
            if 'hash' in new_row:
                del new_row['hash']

        return new_row

    def execute(self, connection):
        rows = []

        if not self.valid:
            return rows

        collection = connection[cromo_database.CROMO_DB][self.real_source]

        if self.sorted_results:
            for row in collection.find(self.query_dict).sort(self.sorted_field, self.sorted_dir):
                new_row = self.process_row(row)
                rows.append(new_row)
        else:
            for row in collection.find(self.query_dict):
                new_row = self.process_row(row)
                rows.append(new_row)

        if self.all_fields and len(rows) > 0:
            for k in self.all_keys:
                if k not in rows[0]:
                    rows[0][k] = 'undefined'

        return rows

def process_query(query_string, debug_only, connection = None):
    """Process the query.
    
    1. select * from users;
    2. select screen_name from users;
    3. select email from users where createdon like 'sumerian'"""

    # 1. Verify it's a somewhat valid query string.    
    if 'from' not in query_string or 'select' not in query_string:
        return []

    sqlobj = Sql2Mongo(query_string.lower())

    if debug_only:
        return sqlobj.getx()

    # 6. Run the query.
    if connection is None:
        with MongoClient(cromo_database.CROMO_URI) as connection:
            rows = sqlobj.execute(connection)
    else:
        rows = sqlobj.execute(connection)

    return rows
