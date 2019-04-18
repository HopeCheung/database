import json
import copy
import csv
import os
import logging

class Index():

    def __init__(self, index_name, table_name, index_columns, kind):
        self._index_name = index_name
        self._columns = index_columns
        self._kind = kind
        self._table_name = table_name

        self._index_data = None

    def compute_key(self, row):

        key_v = [row[k] for k in self._columns]
        key_v = "_".join(key_v)
        return key_v

    def add_to_index(self, row, rid):
        if self._index_data is None:
            self._index_data = {}

        key = self.compute_key(row)
        bucket = self._index_data.get(key, [])
        if self._kind != "INDEX":
            if len(bucket) > 0:
                raise KeyError("Duplicate key")
        bucket.append(rid)
        self._index_data[key] = bucket

    def to_json(self):

        result = {}
        result["name"] = self._index_name
        result["columns"] = self._columns
        result["kind"] = self._kind
        result["index_data"] = self._index_data
        return result

    def matches_index(self, template):
        k = set(list(template.keys()))
        c = set(self._columns)

        if c.issubset(k):
            if self._index_data is not None:
                kk = len(self._index_data.keys())
            else:
                kk = 0
        else:
            kk = None
        return kk

    def find_rows(self, tmp):
        t_keys = tmp.keys()
        t_vals = [tmp[k] for k in self._columns]
        t_s = "_".join(t_vals)

        d = self._index_data.get(t_s, None)

        return d

    def get_no_of_entries(self):
        return len(list(self._index_data.keys()))


class CSVDataTable():

    rel_path = os.path.dirname(os.path.realpath('__file__'))
    csvdir = rel_path + "/../DB/"

    def __init__(self, table_name, column_names=None, primary_key_columns=None, loadit=False):
        self._table_name = table_name
        self._primary_key_columns = primary_key_columns
        self._column_names = column_names

        self._indexes = None

        if not loadit:
            if column_names is None or table_name is None:
                raise ValueError("Did not provide table_name or column_name for table create.")
            self._next_row_id = 1
            self._rows = {}

            if primary_key_columns:
                self.add_index("PRIMARY", self._primary_key_columns, "PRIMARY")

    def get_table_name(self):
        return self._table_name

    def get_rows(self):
        return [self._rows[r] for r in self._rows]

    def add_index(self, index_name, columns, kind):

        if self._indexes is None:
            self._indexes = {}

        # check duplicate indexes
        self._indexes[index_name] = Index(index_name=index_name, table_name= self._table_name,
                                          index_columns=columns, kind=kind)
        self.build(index_name)

    def build(self, i_name):

        idx = self._indexes[i_name]
        for k, v in self._rows.items():
            idx.add_to_index(v, k)

    def insert(self, r):

        if self._rows is None:
            self._rows = {}
        rid = self.get_next_row_id()

        if self._indexes is not None:
            for k,v in self._indexes.items():
                v.add_to_index(r, rid)

        self._rows[rid] = copy.copy(r)

    def get_next_row_id(self):
        self._next_row_id += 1
        return self._next_row_id

    def import_data(self, rows):
        for r in rows:
            self.insert(r)

    def save(self):

        d = {
            "state":{
                "table_name" : self._table_name,
                "primary_key_column" : self._primary_key_columns,
                "columns_name" : self._column_names,
                "next_rid" : self.get_next_row_id()
            }
        }
        fn = CSVDataTable.csvdir+ self._table_name + ".json"
        d["rows"] = self._rows
        d["indexes"] = {}

        for k,v in self._indexes.items():
            idxs = d.get("indexes", {})
            idx_string = v.to_json()
            idxs[k] = idx_string

        d = json.dumps(d, indent=2)
        with open(fn, "w+") as outfile:
            outfile.write(d)

    def load(self):

        fn = CSVDataTable.csvdir + self._table_name + ".json"
        with open(fn, "r") as infile:
            d = json.load(infile)

            state = d["state"]
            self._table_name = state["table_name"]
            self._primary_key_columns = state["primary_key_column"]
            self._column_names = state["columns_name"]
            self._next_row_id = state["next_rid"]
            self._rows = {}
            for elem in d["rows"]:
                self._rows[int(elem)] = d["rows"][elem]

            for k, v in d["indexes"].items():
                idx = Index(v["name"], state["table_name"], v["columns"], v["kind"])
                idx._index_data = v["index_data"]
                if self._indexes is None:
                    self._indexes = {}
                self._indexes[k] = idx


    def get_best_index(self, t):

        best = None
        n = None

        if self._indexes is not None:
            for k,v in self._indexes.items():
                cnt = v.matches_index(t)
                if cnt is not None:
                    if best is None:
                        best = cnt
                        n = k
                    else:
                        if cnt > best:
                            best = len(v.keys())
                            n = k
        return n

    def get_index_and_selectivity(self, cols):

        on_template = dict(zip(cols, [None]*len(cols)))
        best = None
        n  = self.get_best_index(on_template)

        if n is not None:
            best = len(list(self._rows.keys()))/(self._indexes[n].get_no_of_entries())

        return n, best

    def _get_specific_where(self, wc):

        result = {}
        if wc is not None:
            for k,v in wc.items():
                kk = k.split(".")
                if len(kk) ==self._table_name:
                    result[k] = v
                elif kk[0] == self._table_name:
                    result[kk[1]] = v
        if result == {}:
            result = None

        return result

    def _get_specific_project(self, p_clause):

        result = []
        if p_clause is not None:
            for k in p_clause:
                kk = k.split(".")
                if len(kk) == 1:
                    result.append(k)
                elif kk[0] == self._table_name:
                    result.append(kk[1])
        if result == []:
            result = None

        return result

    @staticmethod
    def on_clause_to_where(on_c, r):

        result = {c:r[c] for c in on_c}
        return result

    def load_from_rows(self, table_name, rows):
        if rows:
            self._column_names = list(rows[0].keys())
        else:
            self._column_names = None
        self._next_row_id = 1
        self._rows = {}
        self.import_data(rows)

    def find_by_scan_template(self, template, table):
        if not isinstance(template, dict):
            raise ValueError("Wrong template!")
        new_table = []
        for row in table:
            if all([template[t] == row[t] for t in template]):
                new_table.append(row)

        return new_table

    def find_by_index(self, tmp, idx):
        r = idx.find_rows(tmp)
        res = [self._rows[k] for k in r]
        return res

    def find_by_template(self, tmp, fields=None, use_index=True):

        idx = self.get_best_index(tmp)
        logging.debug("Using index = %s", idx)

        if idx is None or use_index == False:
            result = self.find_by_scan_template(tmp, self.get_rows())
        else:
            idx = self._indexes[idx]
            res = self.find_by_index(tmp, idx)
            result = self.find_by_scan_template(tmp, res)

        new_t = CSVDataTable(table_name="Derived:" + self._table_name, loadit=True)
        new_t.load_from_rows(table_name="Derived:" + self._table_name, rows=result)

        return new_t

    @staticmethod
    def _get_scan_probe(l_table, r_table, on_clause):

        s_best, s_selective = l_table.get_index_and_selectivity(on_clause)
        r_best, r_selective = r_table.get_index_and_selectivity(on_clause)

        result = l_table, r_table

        if s_best is None and r_best is None:
            result = l_table, r_table
        elif s_best is None and r_best is not None:
            result = r_table, l_table
        elif s_best is not None and r_best is None:
            result = l_table, r_table
        elif s_best is not None and r_best is not None and s_selective < r_selective:
            result = r_table, l_table

        return result

    def join(self, r_table, on_clause, w_clause, p_clause, optimize=True):

        s_table, p_table = self._get_scan_probe(self, r_table, on_clause)

        if s_table != self and optimize:
            logging.debug("Swapping tables")
        else:
            logging.debug("Not swapping tables")

        logging.debug("Before pushdowm, scan rows = %s", len(s_table.get_rows()))

        if optimize:
            s_tmp = s_table._get_specific_where(w_clause)
            s_proj = s_table._get_specific_project(p_clause)

            s_rows = s_table.find_by_template(s_tmp, s_proj)
            logging.debug("After pushdown, scan rows = %s", len(s_table.get_rows()))
        else:
            s_rows = s_table

        scan_rows = s_rows.get_rows()

        result = []
        s_tmp = s_table._get_specific_where(w_clause)

        for r in scan_rows:
            if not optimize:
                if not all([s_tmp[t] == r[t] for t in s_tmp]):
                    continue

            p_where1 = CSVDataTable.on_clause_to_where(on_clause, r)
            p_where2 = p_table._get_specific_where(w_clause)
            p_where = copy.copy(p_where1)
            if p_where2 != None:
                p_where.update(p_where2)

            p_project = p_table._get_specific_project(p_clause)
            p_rows = p_table.find_by_template(p_where, p_project)
            p_rows = p_rows.get_rows()

            if p_rows:
                for r2 in p_rows:
                    new_r = {**r, **r2}
                    result.append(new_r)

        tn = "Join(" + self.get_table_name() + "," + r_table.get_table_name() + ")"
        final_result = CSVDataTable(
            table_name=tn,
            loadit=True
        )

        final_result.load_from_rows(table_name=tn, rows=result)

        return final_result

    def delete(self, template):

        find_item = self.find_by_template(template)
        find_rows = find_item._rows

        backup = []
        for row in find_rows.values():
            for rid in self._rows:
                if(row == self._rows[rid]):
                    backup.append(rid)

        for rid in backup:

            self._rows.pop(rid)

        for index in self._indexes:
            for item in self._indexes[index]._index_data:
                for id in backup:
                    if id in self._indexes[index]._index_data[item]:
                        self._indexes[index]._index_data[item].remove(id)















