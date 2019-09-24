from src.RDBDataTable import RDBDataTable
import json

# Connection Information to the Database
connect_info = {
        'host': 'localhost',
        'user': 'dbuser',
        'password': 'dbuserdbuser',
        'db': 'lahman2019raw',
        'port': 3306,
    }


# All Necessary Tests

def t_find_by_template(table_name, key_cols, fields, tmp):
    rdb_tbl = RDBDataTable(table_name=table_name, connect_info=connect_info, key_columns=key_cols)
    res = rdb_tbl.find_by_template(template=tmp, field_list=fields)
    if res == ():
        print("Find By Template on", table_name, "with template", tmp, ":\n", 'None\n')
    else:
        print("Find By Template on", table_name, "with template", tmp, ":\n",
              json.dumps(res, indent=2), '\n')


def t_find_by_primary_key(table_name, key_cols, fields, keys):
    rdb_tbl = RDBDataTable(table_name, connect_info, key_cols)
    res = rdb_tbl.find_by_primary_key(key_fields=keys, field_list=fields)
    if res == ():
        print("Find By Primary Key on", table_name, "with keys", keys, ":\n", "None\n")
    else:
        print("Find By Primary Key on", table_name, "with keys ", keys, ":\n",
              json.dumps(res, indent=2), '\n')


def t_insert(table_name, key_cols, insert_record):

    rdb_tbl = RDBDataTable(table_name, connect_info, key_cols)
    existing = rdb_tbl.find_by_template(template=insert_record)
    if existing == ():
        existing_len = 0
    else: existing_len = len(existing)
    try:
        res = rdb_tbl.insert(new_record=insert_record)
        check = rdb_tbl.find_by_template(template=insert_record)

        if check == ():
            check_len = 0
        else:
            check_len = len(check)

        if check_len and check_len > existing_len:
            print(existing_len, "record(s) found before insertion and", check_len,
                  "record(s) found after insertion. Insert into",
                  table_name, "was successful\n", insert_record, "\n")
        else:
            print("Record not found - Insert into", table_name, "failed\n")
    except Exception as e:
        print("Insertion of record", insert_record, "unsuccessful due to:\n", e)
        print("Test successful as the insert failed as it should")


def t_delete_by_template(table_name, key_cols, tmp):
    rdb_tbl = RDBDataTable(table_name, connect_info, key_cols)
    res = rdb_tbl.delete_by_template(template=tmp)
    check = rdb_tbl.find_by_template(template=tmp)
    if res > 0 and check:
        print("Record found - Delete by template on ", table_name, "failed:\n", tmp, "\n")
    elif res > 0:
        print("Record not found after deletion - Delete By Template on", table_name,
              "with template\n", tmp, "\nwas successful with the deletion of {} rows".format(res), "\n")
    elif res == 0:
        print("No match found for deletion based on the given template\n", tmp, "\nTest Successful. 0 rows deleted")


def t_delete_by_key(table_name, key_cols, keys):
    rdb_tbl = RDBDataTable(table_name, connect_info, key_cols)
    res = rdb_tbl.delete_by_key(key_fields=keys)
    check = rdb_tbl.find_by_primary_key(key_fields=keys)
    if res > 0 and check:
        print("Record found - Delete by key on ", table_name, "failed:\n", keys, "\n")
    elif res > 0:
        print("Record not found after deletion - Delete By Key on", table_name,
              "with keys\n", keys, "\nwas successful with the deletion of {} rows".format(res), "\n")
    elif res == 0:
        print("No match found for deletion based on the given key", keys, "\nTest Successful. 0 rows deleted")


def t_update_by_template(table_name, key_cols, tmp, updates):
    rdb_tbl = RDBDataTable(table_name, connect_info, key_cols)
    existing = rdb_tbl.find_by_template(template=updates)
    if existing == ():
        existing_len = 0
    else:
        existing_len = len(existing)
    existing_tmp = rdb_tbl.find_by_template(template=tmp)
    if existing_tmp == ():
        existing_tmp_len = 0
    else: existing_tmp_len = len(existing_tmp)
    try:
        res = rdb_tbl.update_by_template(template=tmp, new_values=updates)
        check_updates = rdb_tbl.find_by_template(template=updates)
        if check_updates == ():
            check_updates_len = 0
        else: check_updates_len = len(check_updates)

        if res > 0 and check_updates_len >= existing_len:
            print(existing_len, "record(s) found before updates and", check_updates_len,
                  "record(s) found after updates. Update on",
                  table_name, "with template\n", tmp, "\nand updates", updates, "was successful.\n",
                  "Data found based on updated values:\n", json.dumps(check_updates, indent=2))
        elif res == 0 and existing_tmp_len == 0:
            print("No match found for update based on the given template", tmp,  "\nTest Successful. 0 rows updated")
    except Exception as e:
        print("Update based on template", tmp, "\nand values", updates, "unsuccessful due to:\n", e)
        print("Test successful as the update failed as it should")


def t_update_by_key(table_name, key_cols, keys, updates):
    rdb_tbl = RDBDataTable(table_name, connect_info, key_cols)
    existing = rdb_tbl.find_by_template(template=updates)
    if existing == ():
        existing_len = 0
    else: existing_len = len(existing)
    existing_tmp = rdb_tbl.find_by_primary_key(key_fields=keys)
    if existing_tmp == ():
        existing_tmp_len = 0
    else: existing_tmp_len = len(existing_tmp)
    try:
        res = rdb_tbl.update_by_key(key_fields=keys, new_values=updates)
        check_updates = rdb_tbl.find_by_template(template=updates)
        if check_updates == ():
            check_updates_len = 0
        else: check_updates_len = len(check_updates)

        if res > 0 and check_updates_len >= existing_len:
            print(existing_len, "record(s) found before updates and", check_updates_len,
                  "record(s) found after updates. Update on",
                  table_name, "with keys", keys, "\nand updates", updates, "was successful.\n",
                  "Data found based on updated values:\n", json.dumps(check_updates, indent=2))
        elif res == 0 and existing_tmp_len == 0:
            print("No match found for update based on the given keys", keys, "\nTest Successful. 0 rows updated")

    except Exception as e:
        print("Update based on keys", keys, "\nand values", updates, "unsuccessful due to:\n", e)
        print("Test successful as the update failed as it should")


# Testing using People Table

print("\nTable used for Testing Methods: People")

key_cols_people = ['playerID']
fields_people = ['playerID', 'nameFirst', 'nameLast', 'birthState', 'throws']

print("\nTesting Find by Template with matches in the data:")
template_people = {"nameLast": "Williams", "birthState": "NY"}
t_find_by_template(table_name='people', key_cols=key_cols_people, fields=fields_people, tmp=template_people)

print("\nTesting Find by Template without matches in the data:")
template_people = {"nameLast": "Luke", "birthState": "NJ"}
t_find_by_template(table_name='people', key_cols=key_cols_people, fields=fields_people, tmp=template_people)

print("\nTesting Find by Key with matches in the data:")
key_people = ['willich01']
t_find_by_primary_key(table_name='people',
                      key_cols=key_cols_people, fields=fields_people, keys=key_people)

print("\nTesting Find by Key without matches in the data:")
key_people = ['adam']
t_find_by_primary_key(table_name='people',
                      key_cols=key_cols_people, fields=fields_people, keys=key_people)

print("\nTesting Valid Insert:")
new_record = {'playerID': 'TestPlayer', 'throws': 'R', 'birthYear': '1980'}
t_insert(table_name='people',
         key_cols=key_cols_people, insert_record=new_record)

print("\nTesting Invalid (Duplicate Primary Key) Insert:")
new_record = {'playerID': 'willich01', 'throws': 'R', 'birthYear': '1980'}
t_insert(table_name='people',
         key_cols=key_cols_people, insert_record=new_record)

print("\nTesting Insert without passing the primary key column:")
new_record = {'throws': 'R', 'birthYear': '1980'}
t_insert(table_name='people',
         key_cols=key_cols_people, insert_record=new_record)

print("\nTesting Delete by Template with matches in the data:")
template_people = {"nameLast": "Williams", "birthState": "NY"}
t_delete_by_template(table_name='people', key_cols=key_cols_people, tmp=template_people)

print("\nTesting Delete by Template without matches in the data:")
template_people = {"nameLast": "Luke", "birthState": "NJ"}
t_delete_by_template(table_name='people', key_cols=key_cols_people, tmp=template_people)

print("\nTesting Delete by Key with matches in the data:")
key_people = ['abadan01']
t_delete_by_key(table_name='people', key_cols=key_cols_people, keys=key_people)

print("\nTesting Delete by Key without matches in the data:")
key_people = ['adam']
t_delete_by_key(table_name='people', key_cols=key_cols_people, keys=key_people)

print("\nTesting Update by Template with matches in the data:")
template_people = {"nameLast": "Aaron", "birthState": "AL"}
updates = {"throws": "both", "bats": "R"}
t_update_by_template(table_name='people', key_cols=key_cols_people, tmp=template_people, updates=updates)

print("\nTesting Update by Template with matches in the data but with a duplicate value for primary key column:")
template_people = {"playerID": "zuverge01", "birthState": "MI"}
updates = {"playerID": "adcocjo01", "throws": "both", "bats": "R"}
t_update_by_template(table_name='people', key_cols=key_cols_people, tmp=template_people, updates=updates)

print("\nTesting Update by Template without matches in the data:")
template_people = {"nameLast": "Luke", "birthState": "NJ"}
updates = {"throws": "both", "bats": "R"}
t_update_by_template(table_name='people', key_cols=key_cols_people, tmp=template_people, updates=updates)

print("\nTesting Update by Key with matches in the data:")
key_people = ['allenbo02']
updates = {"birthState": "NY", "bats": "R", 'throws': 'R', 'birthYear': '1965', 'birthCity': 'Brooklyn'}
t_update_by_key(table_name='people',
                key_cols=key_cols_people, keys=key_people, updates=updates)

print("\nTesting Update by Key with matches in the data but with a duplicate value for primary key column:")
key_people = ['alyeabr01']
updates = {"playerID": "ankenpa01", "throws": "L", "bats": "L"}
t_update_by_key(table_name='people',
                key_cols=key_cols_people, keys=key_people, updates=updates)

print("\nTesting Update by Key without matches in the data:")
key_people = ['adam']
updates = {"birthCity": "Houston", "throws": "S"}
t_update_by_key(table_name='people',
                key_cols=key_cols_people, keys=key_people, updates=updates)


# Testing using Batting Table

print("\n\nTable used for Testing Methods: Batting")

key_cols_batting = ['playerID', 'yearID', 'teamID', 'stint']
fields_batting = ['playerID', 'teamID', 'yearID', 'AB', 'H', 'HR', 'RBI']

print("\nTesting Find by Template with matches in the data:")
template_batting = {'teamID': 'BOS', 'yearID': '1960', 'stint':'2'}
t_find_by_template(table_name='batting', key_cols=key_cols_batting, fields=fields_batting, tmp=template_batting)

print("\nTesting Find by Template without matches in the data:")
template_batting = {'teamID': 'TRO', 'yearID': '1970'}
t_find_by_template(table_name='batting', key_cols=key_cols_batting, fields=fields_batting, tmp=template_batting)

print("\nTesting Find by Key with matches in the data:")
key_batting = ['aardsda01', '2004', 'SFN', '1']
t_find_by_primary_key(table_name='batting',
                      key_cols=key_cols_batting, fields=fields_batting, keys=key_batting)

print("\nTesting Find by Key without matches in the data:")
key_batting = ['aardsda01', '2010', 'BOS', '1']
t_find_by_primary_key(table_name='batting',
                      key_cols=key_cols_batting, fields=fields_batting, keys=key_batting)

print("\nTesting Valid Insert:")
new_record = {'playerID': 'TestPlayer', 'yearID': '2011', 'AB': '1', 'stint': '1', 'teamID': 'NYA'}
t_insert(table_name='batting',
         key_cols=key_cols_batting, insert_record=new_record)

print("\nTesting Invalid (Duplicate Primary Key) Insert:")
new_record = {'playerID': 'abbotku01', 'yearID': '2001', 'teamID': 'ATL',  'stint': '1'}
t_insert(table_name='batting',
         key_cols=key_cols_batting, insert_record=new_record)

print("\nTesting Delete by Template with matches in the data:")
template_batting = {'teamID': 'TRO', 'yearID': '1872'}
t_delete_by_template(table_name='batting', key_cols=key_cols_batting, tmp=template_batting)

print("\nTesting Delete by Template without matches in the data:")
template_batting = {'playerID': 'aardsda01', 'yearID': '2010', 'teamID': 'BOS',  'stint': '1'}
t_delete_by_template(table_name='batting', key_cols=key_cols_batting, tmp=template_batting)


print("\nTesting Delete by Key with matches in the data:")
key_batting = ['aardsda01', '2004', 'SFN', '1']
t_delete_by_key(table_name='batting', key_cols=key_cols_batting, keys=key_batting)

print("\nTesting Delete by Key without matches in the data:")
key_batting = ['aardsda01', '2010', 'BOS', '1']
t_delete_by_key(table_name='batting', key_cols=key_cols_batting, keys=key_batting)

print("\nTesting Update by Template with matches in the data:")
template_batting = {'teamID': 'LAA', 'yearID': '2012', 'stint': '2'}
updates = {"R": "1", "2B": "2", "H": '1', 'BB': '1'}
t_update_by_template(table_name='batting', key_cols=key_cols_batting, tmp=template_batting, updates=updates)

print("\nTesting Update by Template with matches in the data but with a duplicate value for primary key column:")
template_batting = {'teamID': 'BOS', 'yearID': '2012'}
updates = {'yearID': '2011', 'stint': '1'}
t_update_by_template(table_name='batting', key_cols=key_cols_batting, tmp=template_batting, updates=updates)

print("\nTesting Update by Template without matches in the data:")
template_batting = {'playerID': 'aardsda01', 'yearID': '2010', 'teamID': 'BOS',  'stint': '1'}
updates = {"R": "1", "2B": "2"}
t_update_by_template(table_name='batting', key_cols=key_cols_batting, tmp=template_batting, updates=updates)

print("\nTesting Update by Key with matches in the data:")
key_batting = ['alexama02', '2006', 'SDN', '1']
updates = {"G": "11", "H": "4", 'BB': '4'}
t_update_by_key(table_name='batting',
                key_cols=key_cols_batting, keys=key_batting, updates=updates)

print("\nTesting Update by Key with matches in the data but with a duplicate value for primary key column:")
key_batting = ['alexama02', '2005', 'SDN', '1']
updates = {"playerID": "adkinjo01", "yearID": "2006"}
t_update_by_key(table_name='batting',
                key_cols=key_cols_batting, keys=key_batting, updates=updates)

print("\nTesting Update by Key without matches in the data:")
key_batting = ['aardsda01', '2010', 'BOS', '1']
updates = {"G": "11", "H": "4", 'BB': '1'}
t_update_by_key(table_name='batting',
                key_cols=key_cols_batting, keys=key_batting, updates=updates)
