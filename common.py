# -*- coding: utf-8 -*-
"""
    Decode PaperCut logs

    Requires pandas 0.11.0 or higher
"""
from __future__ import division
import re, sys, glob, os, time 
import numpy as np
import pandas as pd
from pandas import DataFrame, Series, Timestamp, DateOffset, HDFStore
import matplotlib as mpl
import matplotlib.pyplot as plt
import cPickle as pickle


def versions():
    print '-' * 60
    print 'python:', sys.version
    print 'numpy:', np.__version__
    print 'matplotlib:', mpl.__version__
    print 'pandas:', pd.__version__
    print '-' * 60


def save_object(path, obj):
    """Save obj to path"""
    try: 
        f = open(path, 'wb')
        pickle.dump(obj, f)
        f.close()
    except e:
        print 'save_object', path
        print obj
        print type(obj)
        print len(obj)
        raise e


def load_object(path, default=None):
    """Load object from path"""
    try:
        return pickle.load(open(path, 'rb'))
    except:
        return default


def save_df(path, df):        
    store = HDFStore(path)
    store.put('logs', df)
    store.close()


def load_df(path, default=None):        
    print 'load_df', path, default
    try:
        store = HDFStore(path)
        print store.keys()
        df = store.get('logs')
        store.close()
        return df
    except:
        return default
        
      
def get_name_from_path(path):
    base = os.path.basename(path)
    return os.path.splitext(base)[0]

        
class ObjectDirectory:

    ROOT = 'data'
    
    def __init__(self, name):
        self.name = name
        self.dirs = []
        self.toc = self.get_toc()

    def __repr__(self):
        return 'ObjectDirectory(%s)' % repr(self.__dict__) 

    def get_toc(self, temp=False):
        def get_matches(pattern):
            return glob.glob(os.path.join(self.get_dir(temp), pattern))
        matches = get_matches('*.h5') + get_matches('*.pkl')
        return {get_name_from_path(path): path for path in matches}
    
    def make_dir_if_necessary(self, path):
        dir, _ = os.path.split(path)
        if dir in self.dirs:
            return
        if not os.path.exists(dir):    
            os.makedirs(dir)
        self.dirs.append(dir)

    def get_dir(self, temp):
        dir = os.path.join(ObjectDirectory.ROOT, self.name)
        if temp:
            dir = os.path.join(dir, 'temp')
        return dir
 
    def get_path(self, obj_name, temp=False, is_df=False):
        """Only call this with an extension on obj_name if obj_name is 
            a file
        """
        if obj_name.endswith('.h5') or obj_name.endswith('.pkl'):
            ext = ''
        else:    
            ext = '.h5' if is_df else '.pkl'
        return os.path.join(self.get_dir(temp), obj_name) + ext

    @staticmethod
    def save_object(path, obj):
        is_df = isinstance(obj, DataFrame)
        if is_df:
            save_df(path, obj)
        else:    
            save_object(path, obj)
           
    @staticmethod
    def load_object(path, default=None):
        is_df = path.endswith('.h5')
        print 'is_df', is_df, path
        if is_df:
            return load_df(path, default)
        else:    
            return load_object(path, default)       

    def save(self, obj_name, obj, temp=False):
        is_df = isinstance(obj, DataFrame)
        path = self.get_path(obj_name, temp, is_df)
        self.make_dir_if_necessary(path)
        ObjectDirectory.save_object(path, obj)
        self.toc[obj_name] = obj

    def load(self, obj_name, temp=False, default=None):
        is_df = obj_name.endswith('.h5')
        path = self.get_path(obj_name, temp, is_df)
        print is_df, path
        return ObjectDirectory.load_object(path, default)

    
    