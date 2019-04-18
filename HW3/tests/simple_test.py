import logging
import sys, os, csv
import json
from src import CSVDataTable

logging.basicConfig(level=logging.DEBUG)

def load(fn):

    result = []
    cols = None
    with open(fn, "r") as infile:
        rdr = csv.DictReader(infile)
        cols = rdr.fieldnames
        for r in rdr:
            result.append(r)

    return result, cols

def t1():
    t = CSVDataTable.CSVDataTable(table_name="Test", column_names=['foo', 'bar'], primary_key_columns = ['foo'], loadit=None)
    print("T= ", t)

#t1()

def t2():
    new_r, cols = load("/Users/zhanghaopeng/Desktop/HW3/CSVFile/offices.csv")
    t = CSVDataTable.CSVDataTable(table_name="rings", column_names=cols,
                                  primary_key_columns=['uni'],
                                  loadit=None)
    t.import_data(new_r)
    print("t=", t)

#t2()

def t3():
    print("--------------------------Test compute_keys--------------------------")
    i = CSVDataTable.Index(index_name="Bob", table_name="offices" ,index_columns=["last_name", "first_name"], kind="UNIQUE")
    r = {"last_name":"Hope", "first_name":"Cheung","uni":"hz2558"}
    kv = i.compute_key(r)
    print("KV=", kv)

    i.add_to_index(row=r, rid="3")
    print("I=", i)
    print("\n")

t3()

def t4():
    print("--------------------------Test add_index--------------------------")
    new_r, cols = load("/Users/zhanghaopeng/Desktop/HW3/CSVFile/offices.csv")
    t = CSVDataTable.CSVDataTable(table_name="rings", column_names=cols,
                                  primary_key_columns=['uni'],
                                  loadit=None)
    r = {"last_name":"Hope", "first_name":"Cheung","uni":"hz2558"}
    t.insert(r)
    r["uni"] = "hz2559"
    t.insert(r)
    cols = ["last_name", "first_name"]
    t.add_index("Name", cols, "INDEX")
    print("t=", t._indexes)
    t.save()
    print("\n")

t4()

def t5():
    print("--------------------------Test find_by_template--------------------------")
    new_r, cols = load("/Users/zhanghaopeng/Desktop/HW3/CSVFile/offices.csv")
    t = CSVDataTable.CSVDataTable(table_name="offices", column_names=cols,
                                  primary_key_columns=["uni"],
                                  loadit=False)
    t.import_data(new_r)
    new_t = t.find_by_template({"last_name":"Zhang", "first_name":"Haopeng"})
    print(json.dumps(new_t.get_rows(), indent=2))
    print("\n")
    return new_t

t5()

def t6():
    print("--------------------------Test join--------------------------")
    new_r1, cols1 = load("/Users/zhanghaopeng/Desktop/HW3/CSVFile/offices.csv")
    new_r2, cols2 = load("/Users/zhanghaopeng/Desktop/HW3/CSVFile/offices2.csv")

    t1 = CSVDataTable.CSVDataTable(table_name="offices", column_names=cols1,
                                  primary_key_columns=["uni"],
                                  loadit=False)
    t2 = CSVDataTable.CSVDataTable(table_name="offices2", column_names=cols2,
                                  primary_key_columns=["uni"],
                                  loadit=False)
    t1.import_data(new_r1)
    t2.import_data(new_r2)

    j = t2.join(t1, ['uni'],
                w_clause = {"offices2.Location":"BJ"},
                p_clause = ["uni", "offices.last_name", "office2.first_name", "offices.email", "offices2.Location"])
    print(json.dumps(j.get_rows(), indent=2))
    print("\n")

t6()


def t7():
    print("--------------------------Test delete-------------------------")
    new_r, cols = load("/Users/zhanghaopeng/Desktop/HW3/CSVFile/offices.csv")
    t = CSVDataTable.CSVDataTable(table_name="offices", column_names=cols,
                                  primary_key_columns=["uni"],
                                  loadit=False)

    t.import_data(new_r)
    print("Before deletion:")
    print(json.dumps(t.get_rows(), indent=2))

    t.delete({"last_name":"Zhang", "first_name":"Haopeng"})
    print("After deletion:")
    print(json.dumps(t.get_rows(), indent=2))
    print("\n")

t7()

def t8():
    print("--------------------------Test load-------------------------")
    t = CSVDataTable.CSVDataTable(table_name="rings", column_names=None,
                                  primary_key_columns=None,
                                  loadit=True)
    t.load()
    print("Table rows:")
    print(json.dumps(t.get_rows(), indent=2))
    print("Loaded Table find_by template:")
    new_t = t.find_by_template({"last_name": "Hope", "first_name": "Cheung"})
    print(json.dumps(new_t.get_rows(), indent=2))

t8()



