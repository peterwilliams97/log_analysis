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
    
versions()  


def save_object(path, obj):
    """Save obj to path"""
    pickle.dump(obj, open(path, 'wb'))
    

def load_object(path, default=None):
    """Load object from path"""
    try:
        return pickle.load(open(path, 'rb'))
    except:
        return default


def parse_timestamp(timestamp):
    """Convert a string of the form 2011-03-10 15:10:34,687
      to a pandas TimeStamp
    """
    # Do we need to make this more tolerant of missing values?
    dt, ms = timestamp.split(',')
    return Timestamp(dt) + DateOffset(microseconds=int(ms)*1000)
    
   
# Log lines look like
#	2011-03-10 15:10:34,687 ERROR BaseXMLRPCServlet:110 - Error during XMLRPC request on: client-xmlrpc, IP: 10.203.0.122 [3751531@http-436]
#	2011-03-10 15:10:34 INFO BaseXMLRPCServlet:110 - Error during XMLRPC request on: client-xmlrpc, IP: 10.203.0.122 [3751531@http-436]
#	 Jan 21 09:32:07 DEBUG:  insert_fragments_in_file: (subset 0) offsets={0:519:1,1:75992:0,} [3744]

PATTERN_LOG_LINE = r'''
    (?P<timestamp>\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}(,\d{0,3})?)
    \s+
    (?P<level>ERROR|INFO|DEBUG)
    \s+
    (?P<file>\w+)
    :
    (?P<line>\d+)
    \s+
    -
    \s+
    (?P<content>.*)
    \s*
    \[(?P<thread>.+?)\]
'''

ENTRY_KEYS = re.findall(r'\?P<(\w+)>', PATTERN_LOG_LINE)
RE_LOG_LINE = re.compile(PATTERN_LOG_LINE, re.IGNORECASE|re.DOTALL|re.VERBOSE)


def decode_log_line(line):
    """ Return a list of the parts of a server.log line
        See PATTERN_LOG_LINE for the parts
    """
    m = RE_LOG_LINE.search(line)
    if not m:
        return None
    d = m.groupdict() 
    return [
        parse_timestamp(d.get('timestamp', '')),
        d.get('level'),
        d.get('file'),
        int(d.get('line', '-1')),
        d.get('content', '[EMPTY]')[:256],
        d.get('thread')
    ] 


def get_entries(log_file):
    """Generator 
        Returns decoded log entries for all well-formed log entries in a log file"""
    with open(log_file, 'rt') as f:
        for i,line in enumerate(f):
            entry = decode_log_line(line)
            if entry:
                yield  entry

    
def log_file_to_df(log_file):
    """Returns a pandas dataframe whose rows are the decoded entries of the lines in log_file"""
    # Why can't we construct a DataFrame with a generator?
    entries = [e for e in get_entries(log_file)]
    df = DataFrame(entries, columns = ENTRY_KEYS)
    del entries
    return df


USEC = DateOffset(microseconds=1)

def make_timestamps_unique(df):
    """Make all the timestamps in DataFrame df unique by making each
        timestamp at least 1 µsec greater than timestamp of preceeding row
    """
    for i in range(1, len(df)):
        if df.ix[i,'timestamp'] <= df.ix[i-1,'timestamp']:
            df.ix[i,'timestamp'] = df.ix[i-1,'timestamp'] + USEC


def load_log(log_path):
    """Return a pandas DataFrame for all the valid log entry lines in log_file
        The index of the DataFrame are the uniqufied timestamps of the log entries
    """
    print 'load_log(%s)' % log_path
    df = log_file_to_df(log_path)
    make_timestamps_unique(df)
    df = df.set_index('timestamp')
    return df

    
class HdfStore:

    def __init__(self, store_path):
        self.store = HDFStore(store_path)
        #self.store['logs'] = {}
        #self.store['history'] = {}

    def save_log(self, log_path):
        """Return a pandas DataFrame for all the valid log entry lines in log_file
            The index of the DataFrame are the uniqufied timestamps of the log entries
        """
        history = load_object('temp.pkl', {})
        if log_path in history:
            return
        
        print 'Processing %s' % log_path,
        start = time.time()
        df = load_log(log_path)
        self.store.append('logs', df)
        load_time = time.time() - start
        
        history[log_path] = {
            'start': df.index[0],
            'end': df.index[-1],
            'load_time': load_time
        }
        save_object('temp.pkl', history)
        del df
        history[log_path]

    def get_log_paths(self):
        history = load_object('temp.pkl', {})
        sorted_keys = history.keys()
        print history
        print type(sorted_keys)
        sorted_keys.sort(key=lambda k: history[k]['start'])
        return [(k,history[k]) for k in sorted_keys]
 

def load_log_pattern(hdf_path, path_pattern):  
    
    path_list = glob.glob(path_pattern) 
    print path_list
    if not path_list:
        return
    
    hdf_store = HdfStore('temp.h5')
    for path in path_list:
        hdf_store.save_log(path)
    
    log_history = hdf_store.get_log_paths() 
    for i,(path,history) in enumerate(log_history[:1]):
        print '%2d: %s  ---  %s : %s' % (i, history['start'], history['end'], path)

    p0,h0 = log_history[0]
    for p1,h1 in log_history[1:]:
        assert h0['end'] < h1['start'], '\n%s %s\n%s %s' % (p0,h0, p1,h1)
    
    paths = [p for p,_ in log_history]
        
    final_store = HDFStore(hdf_path)
    print
    print hdf_store.store.keys()
    print type(hdf_store.store['logs'])
    print type(hdf_store.store.logs)
   
    final_store.put(hdf_store.logs[paths[0]])
    for path in paths[1:]:
        final_store.append(hdf_store.log[path])


hdf_path = sys.argv[1]  
path_pattern = sys.argv[2]   
load_log_pattern(hdf_path, path_pattern)

