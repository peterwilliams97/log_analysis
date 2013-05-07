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
    #print 'load_log(%s)' % log_path
    df = log_file_to_df(log_path)
    make_timestamps_unique(df)
    df = df.set_index('timestamp')
    return df


class LogSaver:

    @staticmethod
    def normalize(name):
        return name.replace('\\', '_').replace('.', '_')

    def __init__(self, store_path, log_list):
        self.store_path = store_path
        self.log_list = tuple(sorted(log_list))
        hsh = hash(self.log_list)
        sgn = 'n' if hsh < 0 else 'p'
        temp = 'temp_%s%08X' % (sgn, abs(hsh))
        self.progress_store_path = '%s.h5' % temp
        self.history_path = '%s.pkl' % temp
        self.history = load_object(self.history_path, {})

    def __repr__(self):
        return '\n'.join('%s: %s' % (k,v) for k,v in self.__dict__.items())
        
    def save_all_logs(self, force=False):
         
        if os.path.exists(self.store_path):
            return
        if not force:
            assert not os.path.exists(self.history_path), '''
                %s exists but %s does not
                There appears to be a partical conversion
            ''' % (self.history_path, self.store_path)
            
        self.progress_store = HDFStore(self.progress_store_path)
        for log_path in self.log_list:
            self.save_log(log_path)
        
        self.check()    
        print '--------'
        print 'All tables in %s' % self.progress_store_path
        print self.progress_store.keys()
        print '--------'
        
        df_list = [self.progress_store.get(LogSaver.normalize(path)) for path in self.log_list]     
        self.progress_store.close()
        print 'Closed %s' % self.progress_store_path
        
        df_all = pd.concat(df_list)
        print 'Final list has %d entries' % len(df_all)
        final_store = HDFStore(self.store_path)
        final_store.put('logs', df_all)
        final_store.close()
        print 'Closed %s' % self.store_path

    def test_store(self):    
        final_store = HDFStore(self.store_path)
        print '----'
        print final_store.keys()
        print '-' * 80
        logs = final_store['/logs']
        print type(logs)
        print len(logs)
        print logs.columns
        final_store.close()

    def cleanup(self):    
        os.remove(self.progress_store_path)
        os.remove(self.history_path)

    def save_log(self, path):
        """Return a pandas DataFrame for all the valid log entry lines in log_file
            The index of the DataFrame are the uniqufied timestamps of the log entries
        """
        if path in self.history:
            return
        
        print 'Processing %s' % path,
        start = time.time()
        df = load_log(path)
        self.progress_store.put(LogSaver.normalize(path), df)
        load_time = time.time() - start
        
        self.history[path] = {
            'start': df.index[0],
            'end': df.index[-1],
            'load_time': int(load_time),
            'num': len(df)
        }
        save_object(self.history_path, self.history)
        del df
        print self.history[path],
        print '%d of %d' % (len(self.history), len(self.log_list))
        
    def check(self):
        history = load_object(self.history_path, {})
        sorted_keys = history.keys()
        sorted_keys.sort(key=lambda k: history[k]['start'])
        print '-' * 80
        print 'Time range by log file'
        for i,path in enumerate(sorted_keys):
            hist = history[path]
            print '%2d: %s  ---  %s : %s' % (i, hist['start'], hist['end'], path)
        
        path0 = sorted_keys[0]
        for path1 in sorted_keys[1:]:
            hist0,hist1 = history[path0],history[path1] 
            assert hist0['end'] < hist1['start'], '\n%s %s\n%s %s' % (
                path0, hist0, 
                path1, hist1)    
 

def load_log_pattern(hdf_path, path_pattern):  

    path_list = glob.glob(path_pattern) 
    print path_list
    if not path_list:
        return
        
    log_saver = LogSaver(hdf_path, path_list)
    print
    print log_saver
    print
    log_saver.save_all_logs(force=True)

    
def main():
    hdf_path = sys.argv[1]  
    path_pattern = sys.argv[2]   
    load_log_pattern(hdf_path, path_pattern)


if __name__ == '__main__':
    main()