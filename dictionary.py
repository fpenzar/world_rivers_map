import os,os.path
from collections import UserDict
from sqlite3 import dbapi2 as sqlite
import json

# TODO waay to much db access
# code modified from: http://sebsauvage.net/python/snyppets/index.html#dbdict

class dbdict():
    ''' dbdict, a dictionnary-like object for large datasets (several Tera-bytes) '''
   
    def __init__(self,dictName):
        self.db_filename = "dbdict_%s.sqlite" % dictName
        self._internal_dict = dict()
        self._write_dict = dict()
        self._read_dict = dict()
        self.MAX_ELEMENTS = 10000
        if not os.path.isfile(self.db_filename):
            self.con = sqlite.connect(self.db_filename)
            self.con.execute("create table data (key PRIMARY KEY,value)")
        else:
            self.con = sqlite.connect(self.db_filename)
   
    def __getitem__(self, key):
        if key in self._read_dict:
            return self._read_dict[key]
        if key in self._write_dict:
            result = self._write_dict[key]
        else:
            row = self.con.execute("select value from data where key=?",(key,)).fetchone()
            if not row: raise KeyError
            result = json.loads(row[0])
        # clear cache if overused
        if len(self._read_dict) >= self.MAX_ELEMENTS:
            print("clearing")
            self._read_dict.clear()
        self._read_dict[key] = result
        return result
    
    def __contains__(self, key):
        # check in cache first
        if key in self._read_dict:
            return True
        if key in self._write_dict:
            return True
        row = self.con.execute("select value from data where key=?",(key,)).fetchone()
        if not row: return False
        return True
   
    def __setitem__(self, key, item):
        item_str = json.dumps(item)
        # check cache first
        if key in self._internal_dict:
            self.con.execute("update data set value=? where key=?",(item_str,key))
        # check db
        elif self.con.execute("select key from data where key=?",(key,)).fetchone():
            self.con.execute("update data set value=? where key=?",(item_str,key))
        # insert into db if not already exists
        else:
            self.con.execute("insert into data (key,value) values (?,?)",(key, item_str))
        # clear cache if overused
        if len(self._internal_dict) >= self.MAX_ELEMENTS:
            print("clearing")
            self._internal_dict.clear()
        # update cache  
        self._internal_dict[key] = item
        self.con.commit()

    def __delitem__(self, key):
        if key in self._internal_dict:
            del self._internal_dict[key]
            self.con.execute("delete from data where key=?",(key,))
            self.con.commit()
        elif self.con.execute("select key from data where key=?",(key,)).fetchone():
            self.con.execute("delete from data where key=?",(key,))
            self.con.commit()
        else:
             raise KeyError
    

    def flush(self, key):
        # assumes key is already in db
        if key not in self._internal_dict:
            raise KeyError
        item_str = json.dumps(self._internal_dict[key])
        self.con.execute("update data set value=? where key=?",(item_str,key))
        self.con.commit()

            
    def keys(self):
        return [row[0] for row in self.con.execute("select key from data").fetchall()]
    

    def __del__(self):
        self.con.close()
