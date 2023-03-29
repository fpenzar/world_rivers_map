import os, os.path
from sqlite3 import dbapi2 as sqlite
import json

# TODO waay to much db access
# code modified from: http://sebsauvage.net/python/snyppets/index.html#dbdict

class dbdict():
    ''' dbdict, a dictionnary-like object for large datasets (several Tera-bytes) '''
   
    def __init__(self,dictName, max_elements=10000):
        self.db_filename = "dbdict_%s.sqlite" % dictName
        self._write_dict = dict()
        self._read_dict = dict()
        self.max_elements = max_elements
        if not os.path.isfile(self.db_filename):
            self.con = sqlite.connect(self.db_filename)
            self.con.execute("create table data (key PRIMARY KEY,value)")
        else:
            self.con = sqlite.connect(self.db_filename)


    def __getitem__(self, key):
        # returns a reference from the _write_dict
        if key in self._write_dict:
            return self._write_dict[key]
        else:
            row = self.con.execute("select value from data where key=?", (key,)).fetchone()
            if not row: raise KeyError
            result = json.loads(row[0])
        # clear cache if overused
        if len(self._write_dict) >= self.max_elements:
            self.flush()
            self._write_dict.clear()
        self._write_dict[key] = result
        return result
    

    def fast_get(self, key):
        # the result should not be modified
        if key in self._read_dict:
            return self._read_dict[key]
        else:
            row = self.con.execute("select value from data where key=?", (key,)).fetchone()
            if not row: raise KeyError
            result = json.loads(row[0])
        # clear cache if overused
        if len(self._read_dict) >= self.max_elements:
            self._read_dict.clear()
        self._read_dict[key] = result
        return result


    def __contains__(self, key):
        # check in cache first
        if key in self._read_dict:
            return True
        if key in self._write_dict:
            return True
        row = self.con.execute("select value from data where key=?", (key,)).fetchone()
        if not row: return False
        return True
    

    def flush(self):
        # insert into db
        for dict_key, dict_value in self._write_dict.items():
            self.con.execute("insert or replace into data (key,value) values (?,?)", (dict_key, json.dumps(dict_value)))
        self.con.commit()
    

    def __setitem__(self, key, item):
        if key not in self._write_dict:
            if len(self._write_dict) >= self.max_elements:
                self.flush()
                self._write_dict.clear()
        if key not in self._read_dict:
            if len(self._read_dict) >= self.max_elements:
                self._read_dict.clear()
        self._write_dict[key] = item
        self._read_dict[key] = item


    def __delitem__(self, key):
        exists = False
        if key in self._read_dict:
            exists = True
            del self._read_dict[key]
        if key in self._write_dict:
            exists = True
            del self._write_dict[key]
        if self.con.execute("select key from data where key=?", (key,)).fetchone():
            exists = True
            self.con.execute("delete from data where key=?", (key,))
            self.con.commit()
        if not exists:
             raise KeyError

            
    def keys(self):
        return [row[0] for row in self.con.execute("select key from data").fetchall()]
    

    def __del__(self):
        for dict_key, dict_value in self._write_dict.items():
            self.con.execute("insert or replace into data (key,value) values (?,?)", (dict_key, json.dumps(dict_value)))
        self.con.commit()
        self.con.close()
