# -*- coding: utf-8 -*-
"""
    Convert PaperCut logs to a panda DataFrame and save in a HDF5 file

    Requires pandas 0.11.0 or higher
"""
from __future__ import division
import re, sys, glob, os 
import numpy as np
import pandas as pd
from pandas import DataFrame, Series, Timestamp, DateOffset
import matplotlib as mpl
import matplotlib.pyplot as plt

def versions():
    print '-' * 60
    print 'VERSIONS'
    print '-' * 60
    print 'python:', sys.version
    print 'numpy:', np.__version__
    print 'matplotlib:', mpl.__version__
    print 'pandas:', pd.__version__
    print '-' * 60
    
versions()  
  

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
    ^
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
    $
'''

ENTRY_KEYS = re.findall(r'\?P<(\w+)>', PATTERN_LOG_LINE)
RE_LOG_LINE = re.compile(PATTERN_LOG_LINE, re.IGNORECASE|re.DOTALL|re.VERBOSE)

def decode_log_line(line):
    """ Return a list of the parts of a server.log line
        See PATTERN_LOG_LINE for the parts
    """
    m = RE_LOG_LINE.match(line.strip('\n').strip())
    if not m:
        return None
    d = m.groupdict() 
    return [
        parse_timestamp(d.get('timestamp', '')),
        d.get('level'),
        d.get('file'),
        int(d.get('line', '-1')),
        d.get('content'),
        d.get('thread')
    ] 

def get_entries(log_file):
    """Generator 
        Returns decoded log entries for all well-formed log entries in a log file"""
    with open(log_file, 'rt') as f:
        for i,line in enumerate(f):
            entry = decode_log_line(line)
            if entry:
                yield [log_file,i] + entry


def log_file_to_df(log_file):
    """Returns a pandas dataframe whose rows are the decoded entries of the lines in log_file"""
    # Why can't we construct a DataFrame with a generator?
    entries = [e for e in get_entries(log_file)]
    return DataFrame(entries, columns = ['logfile', 'logline'] + ENTRY_KEYS)

USEC = DateOffset(microseconds=1)

def make_timestamps_unique(df):
    """Make all the timestamps in DataFrame df unique by making each
        timestamp at least 1 µsec greater than timestamp of preceeding row
    """
    for i in range(1, len(df)):
        if df.ix[i,'timestamp'] <= df.ix[i-1,'timestamp']:
            df.ix[i,'timestamp'] = df.ix[i-1,'timestamp'] + USEC


def load_log(log_file):
    """Return a pandas DataFrame for all the valid log entry lines in log_file
        The index of the DataFrame are the uniquified timestamps of the log entries
    """
    print 'load_log(%s)' % log_file
    df = log_file_to_df(log_file)
    make_timestamps_unique(df)
    return df.set_index('timestamp')
  

def print_row(df, ts):
    eix = df.ix
    msg = '%s:%d - %s' % (eix[ts, 'file'], eix[ts, 'line'], eix[ts, 'content'])
    #print '%-30s %-20s %10s %s' % (ts, eix[ts, 'logfile'], eix[ts, 'logline'], msg[:60])
    print ('%s %-5s %s [%s]' % (ts,  eix[ts, 'level'], msg, eix[ts, 'thread']))[:160]

    
def all_logs_to_df(path_pattern):  
    paths = glob.glob(path_pattern) 
    dfs = [load_log(fn) for fn in paths]
    dfs.sort(key = lambda x: x.index[0])
    for i,df in enumerate(dfs):
        print '%2d: %s  ---  %s' % (i, df.index[0], df.index[-1])

    dfall = pd.concat(dfs)
    for df in dfs:
        print len(df)
    print sum(len(df) for df in dfs), len(dfall)
    print dfall

    for i in range(1,len(dfs)):
        dfs0 = dfs[i-1]
        dfs1 = dfs[i]    
        assert dfs0.index[-1] < dfs1.index[0], '\n%s\n%s' % (dfs0, dfs1)
    return dfall


def all_logs_to_hdf(path_pattern, hdf_file, table):
    try:
        os.remove(hdf_file)
    except:
        pass
    pc_logs = all_logs_to_df(path_pattern)
    pc_logs.to_hdf(hdf_file, table, append=False)


path_pattern = r'server\server.log*'  
all_logs_to_hdf(path_pattern, 'pc_pandas.h5', 'logs')  
  
    

    
    

