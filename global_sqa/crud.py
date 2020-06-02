### crud.py ###
# create read update delete for movies database

import os
import sys
import datetime

import numpy as np
import re

import timeit
from contextlib import contextmanager

from sqlalchemy import create_engine, Table, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import StatementError
from sqlalchemy.ext.automap import automap_base

from sqa_config import DATABASE_URI

import pprint
from itertools import combinations 
from scipy.sparse.csgraph import connected_components


global_debug_level = 0

def debug_print(string, pp=None, level=1):
    if global_debug_level:
        if level < global_debug_level:
            if pp is None:
                print(string)
            else:
                pp.pprint(string)

def parse_sql_file(file_):
    """
    """
    import sqlparse
    with open(file_, "r", encoding="utf8") as f:
        data = f.read()
    return sqlparse.split(data)

def create_from_sql_dump(sqa_engine, dump_file):
    """takes a sql dump file, and renerates it with a specific sqa engine
    Excepts StatementErrors and prints a list of failed queries as the end
    """
    queries = parse_sql_file(dump_file)
    failed_q = []
    with sqa_engine.connect() as con:
        for q in queries:
            try:
                con.execute(text(q))
            except StatementError:
                failed_q.append(q)

        print("Failed Queries:")
        for q in failed_q:
            print(q)

@contextmanager
def session_scope(engine):
    """context manager for the session, given an engine. do:
    with session_scope(engine) as s:
        s.query(...) ...
    """
    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()

def build_movies_table(engine):
    """calls all the right stuff to build this specific table.
    some other table build options are commented out here
    """
    sql_file = os.path.join('..', 'global_copy.sql')
    # sql_file = os.path.join('..', '2019_bollywood.sql')
    # sql_file = os.path.join('..', 'FULLBOLLYWOOD.sql')
    create_from_sql_dump(engine, dump_file=sql_file)


def find_nulls(orm_tuple_list):
    """ takes in a list of ORM tuples, and creates a dictionary
    for each one with the whether or not each attribute is null.
    highly scoped for the global-movies table
    """
    tuple_nulls = {}
    for t in orm_tuple_list:
        nulls = {}
        nulls['year'] = True if t.year is None else False
        nulls['genre'] = True if t.genre is None else False
        nulls['director'] = True if t.director is None else False
        nulls['country'] = True if t.country is None else False
        nulls['casts'] = True if t.casts is None else False
        nulls['description'] = True if t.description is None else False
        tuple_nulls[t.id] = nulls
    return tuple_nulls

def get_null_value(null_truth_dict, value_dict):
    """ takes in a dictionary of values from an ORM tuple and a dictionary
    of the relative information weight for each attribute. Calculates a
    theoretical information value - how much information the tuple would
    hold if all non-NULL attributes have meaningful values
    """
    value = 0
    for key, is_null in null_truth_dict.items():
        if not is_null:
            value += value_dict[key]
    return value
        
def norm_val_dict(dict_):
    """normalizes the values of an atomic dictionary
    so that the sum of all items = 1; assumes all items are float
    """
    dsum = sum([v for v in dict_.values()])
    for d, v in dict_.items():
        dict_[d] = v / dsum
    return dict_

def calc_match_val(orm_tup1, orm_tup2, value_dict):
    """ This takes in two ORM tuples and a dictiony of the relative 
    value for each attribute; applies somewhat arbitrary matching to 
    tuples (int distance, string comparisions, and edit distances).
    Calculates a liklihood of match [0-1].
    """
    match_val = 0

    # check int distance for year, after 2 years no value awarded
    if orm_tup1.year is None or orm_tup2.year is None:
        match_val += value_dict['year']
    else:
        year_sep_max = 2
        try:
            year_val = np.max(1 - np.abs(int(orm_tup1.year) - int(orm_tup2.year))/year_sep_max, 0)
        except:
            year_val = .5
        match_val += year_val * value_dict['year']

    # direct string comparision for country
    if orm_tup1.country is None or orm_tup2.country is None:
        match_val += value_dict['country']
    else:
        if orm_tup1.country == orm_tup2.country:
            match_val += value_dict['country']

    # direct string comparision for director
    if orm_tup1.director is None or orm_tup2.director is None:
        match_val += value_dict['director']
    else:
        if orm_tup1.director == orm_tup2.director:
            match_val += value_dict['director']

    # checks the percent of missing cast from each cast lists, takes the minimal
    if orm_tup1.casts is None or orm_tup2.casts is None:
        match_val += value_dict['casts']
    else:
        cast_set1 = set(re.split('; |, |\*|\n', orm_tup1.casts))
        cast_set2 = set(re.split('; |, |\*|\n', orm_tup2.casts))
        
        cast_union = cast_set1.union(cast_set2)
        missing_cast1 = cast_set1.difference(cast_set2)
        missing_cast2 = cast_set2.difference(cast_set1)
        
        percent_inc1 = 1 - (len(missing_cast1)/len(cast_union))
        percent_inc2 = 1 - (len(missing_cast2)/len(cast_union))
        percent_inc = max(percent_inc1, percent_inc2)
        percent_inc = max(min(percent_inc, 1), 0)

        match_val += percent_inc * value_dict['casts']

    # checks a string edit distance against the entire distance for a relative sameness
    if orm_tup1.description is None or orm_tup2.description is None:
        match_val += value_dict['description']
    else:
        pass
        # TODO: String edit

    return match_val

def compare_similar_tuples(orm_tuple_list):
    """Takes a list of ORM tuples with the same title, and checks for matches.
    Returns a dictionary of {[tuple_id]: relative_value} where relative_value
    is the ammount of information in the tuple [0-1], and a dictionary of
    {[(id1, id2)]: match_value}, where match_value is the liklihood of tuples
    being from the same object
    """
    tuple_nulls = find_nulls(orm_tuple_list)
    tup_value_weights = norm_val_dict({
        'year' :  10,
        'genre' : 1,
        'director' : 3,
        'country' : 5,
        'casts' : 1,
        'description' : 1
    })

    information_value = {}
    for tup in orm_tuple_list:
        tup_id = tup.id
        information_value[tup_id] = get_null_value(tuple_nulls[tup_id], tup_value_weights)

    comb = combinations(orm_tuple_list, 2)

    match_dict = {}
    for (t1, t2) in comb:
        match_val = calc_match_val(t1, t2, tup_value_weights)
        match_dict[(t1.id, t2.id)] = match_val

    pp = pprint.PrettyPrinter(indent=4)
    debug_print("\nNULL DICT: ")
    debug_print(tuple_nulls, pp)    
    debug_print("\nNULL VALS: ")
    debug_print(information_value, pp)
    debug_print("\nMATCH CALC: ")
    for key, val in match_dict.items():
        debug_print(key)
        debug_print(val)

    return information_value, match_dict


def search_tuple_duplicates(orm_tuple_list):
    """ Takes a list of tuples with the same or similar names.  Finds an
    information value and match percentage, then decides which tuples to mark
    as duplicates
    """
    # get information value, and unitary matching probablities
    information_value, matching_values = compare_similar_tuples(orm_tuple_list)

    ids = [x.id for x in orm_tuple_list]

    thresh_ = .70
    for key, value in matching_values.items():
        if value > thresh_:
            matching_values[key] = 1
        else:
            matching_values[key] = 0

    # create graph and search for components
    num_nodes = len(ids)
    dense_graph = np.zeros((num_nodes, num_nodes))
    for key, value in matching_values.items():
        id0 = ids.index(key[0])
        id1 = ids.index(key[1])
        dense_graph[id0, id1] = value
        dense_graph[id1, id0] = value

    n_components, labels = connected_components(csgraph=dense_graph, directed=False, return_labels=True)

    # construct matching lists
    replacements = {}

    for n in range(n_components):
        related_ids = [(ids[i], information_value[ids[i]]) for i, label in enumerate(labels) if label == n]
        related_ids.sort(key = lambda x: -x[1])
        replacements[related_ids[0][0]] = [x[0] for x in related_ids[1:]]

    return replacements

def create_progressbar(val, rang, num_blocks):
    """creates a progress bar with #/- and a percentage
    i.e. | #####----- | 50%
    """
    val = max(min(val, rang), 0)
    num_on = int(val/rang * num_blocks) + 1
    num_off = int(num_blocks - num_on)
    string = '   |'
    for i in range(num_on):
        string += '#'
    for i in range(num_off):
        string += '-'
    string += '|\t'
    string += '{0:.0f}%'.format((val/rang)*100)
    return string

def __main__build_database():
    """ MAIN FUNCTION
    creates the database from a .sql text file
    """
    engine = create_engine(DATABASE_URI)
    build_movies_table(engine)

def __main__find_and_mark_dupicates():
    """ MAIN FUNCTION   
    runs over database and seraches for duplicates. marks duplicate column as true or false
    """
    engine = create_engine(DATABASE_URI)

    sys.stdout = open(os.devnull, 'w')      # disable prints
    from reflected_models import Global
    sys.stdout = sys.__stdout__             # enable prints

    duplicated_titles = []
    with session_scope(engine) as s:
        # tuples = s.query(Global).order_by(Global.title.asc()).limit(1000)
        tuples = s.query(Global).order_by(Global.title.asc())

        number_to_process = tuples.count()
        number_of_progress_bars = 50
        processed_count = 0

        number_of_duplicates = 0
        for r in tuples:
            processed_count += 1

            if r.duplicate is not None:
                continue
            
            if r.title in duplicated_titles:
                continue
            
            similar = s.query(Global).filter(Global.title==r.title).all()
            if len(similar) > 1:
                duplicated_titles.append(similar[0].title)
                related = search_tuple_duplicates(similar)
                
                originals = []
                duplicates = []
                for key, item in related.items():
                    originals.append(key)
                    duplicates.extend(item)

                for orig in originals:
                    entry = s.query(Global).filter(Global.id==orig).first()
                    entry.duplicate = False
                for dup in duplicates:
                    entry = s.query(Global).filter(Global.id==orig).first()
                    entry.duplicate = True
                    number_of_duplicates += 1
            
            bar = create_progressbar(processed_count, number_to_process, number_of_progress_bars) + '\r'
            sys.stdout.write(bar)
            sys.stdout.flush()
        
        print()
        print(number_of_duplicates, " duplicates found and marked\nDONE")

if __name__ == '__main__':
    # __main__build_database()
    __main__find_and_mark_dupicates()
