# -*- coding: utf-8 -*-
"""
    Decode PaperCut logs

    Requires pandas 0.11.0 or higher
"""
from __future__ import division
import re, sys, glob 
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
    df = DataFrame(entries, columns = ['logfile', 'logline'] + ENTRY_KEYS)
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

def load_log(log_file):
    """Return a pandas DataFrame for all the valid log entry lines in log_file
        The index of the DataFrame are the uniqufied timestamps of the log entries
    """
    print 'load_log(%s)' % log_file
    df = log_file_to_df(log_file)
    make_timestamps_unique(df)
    df = df.set_index('timestamp')
    return df

def print_row(df, ts):
    eix = df.ix
    msg = '%s:%d - %s' % (eix[ts, 'file'], eix[ts, 'line'], eix[ts, 'content'])
    #print '%-30s %-20s %10s %s' % (ts, eix[ts, 'logfile'], eix[ts, 'logline'], msg[:60])
    print ('%s %-5s %s [%s]' % (ts,  eix[ts, 'level'], msg, eix[ts, 'thread']))[:160]

def test():    
    df = load_log(r'server\server.log')

    print df
    print df.head()
    print df.iloc[0]

    ts = df.index #['timestamp']
    last = ts[0]
    for x in ts[1:]:
        assert x > last, '\n\t%s\n\t%s' % (last, x)
        last = x

    print '-' * 80    
    errors = df[df.level=='INFO']
    print errors
    print errors.head()
    print '-' * 40
    print errors.iloc[0]

    print    
    print df.index.min()
    print df.index.max()    
    print df.index.max() - df.index.min()

def test2():  
    #import gc
    #gc.disable
    files = glob.glob(r'server.log*') # [:2]
    dfs = [load_log(fn) for fn in files]
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

def test3():
    import os
    dfall = test2()
    os.remove('server_logs.h5')
    dfall.to_hdf('server_logs.h5', 'table', append=False)
    del dfall 
    
    df = pd.read_hdf('server_logs.h5', 'table')
    
    print '-' * 80    
    errors = df[df.level=='ERROR']
    print errors
    print errors.head()
    print '-' * 40
    print errors.iloc[0]
    
def test4():
    df = pd.read_hdf('server_logs.h5', 'table')

    #print '-' * 80  

    errors = df[df.level=='ERROR']
    eix = errors.ix
    
    if False:
        print errors
        print errors.head()
        print '-' * 40
        print errors.iloc[0]  

        print '=' * 40
        print 'All errors' 
        print '%-30s %-20s %10s %s' % ('timestamp', 'log file', 'log line', 'message')
       
        for ts in errors.index:
            msg = '%s:%d %s' % (eix[ts, 'file'], eix[ts, 'line'], eix[ts, 'content'])
            print '%-30s %-20s %10s %s' % (ts, eix[ts, 'logfile'], eix[ts, 'logline'], msg[:60])
            
    contents = [eix[ts, 'content'] for ts in errors.index]

    print 
    print '%d log entries' % len(df)    
    print '%d ERROR level entries' % len(contents)
    print '%d unique ERROR level entries' % len(set(contents))
    print 
    for content in sorted(set(contents)):
        print content[:160]
    
    
def test5():
    df = pd.read_hdf('server_logs.h5', 'table')    
    
    print df.ix['2013-04-23 09:00':'2013-04-23 7:00'].index
    for ts in df.ix['2013-04-23 19:00':'2013-04-24 1:00'].index:
        print_row(df, ts)
    
    print df.content.at_time('2013-04-23 21:00:00.014000')

    filtered = df.between_time('2013-04-23 19:00', '2013-04-24 1:00')    
    print filtered
    
    print sorted(set(df.level))
    error = df[df.level=='ERROR']
    info = df[df.level=='INFO']
    debug = df[df.level=='DEBUG']
    
    print len(error)
    print len(info)
    print len(debug)
    print len(error) + len(info) + len(debug), len(df)
    
    
    if True:
        bars = df.logfile.resample('600min', how='count')
        print bars.head() 
        print len(bars), type(bars)
        print bars.describe()
   
        bars_e = df[df.level=='ERROR'].logfile.resample('600min', how='count')
        bars_i = df[df.level=='INFO'].logfile.resample('600min', how='count')
        bars_d = df[df.level=='DEBUG'].logfile.resample('600min', how='count')
        
        bars.plot()
        if True:
            bars_e.plot()
            bars_i.plot()
            bars_d.plot()
        plt.show()

test5()


