"""
Decode PaperCut logs

Log lines look like
	2011-03-10 15:10:34,687 ERROR BaseXMLRPCServlet:110 - Error during XMLRPC request on: client-xmlrpc, IP: 10.203.0.122 [3751531@http-436]
	2011-03-10 15:10:34 INFO BaseXMLRPCServlet:110 - Error during XMLRPC request on: client-xmlrpc, IP: 10.203.0.122 [3751531@http-436]
	 Jan 21 09:32:07 DEBUG:  insert_fragments_in_file: (subset 0) offsets={0:519:1,1:75992:0,} [3744]

Created on 24/3/2011

@author: peter
"""
import re, sys 
import numpy as np
import pandas as pd
from pandas import DataFrame, Series, Timestamp, DateOffset


KEY_TIMESTAMP = 'timestamp'
KEY_LEVEL = 'level'
KEY_CONTENT = 'content'
KEY_THREAD = 'thread'
ENTRY_KEYS = [KEY_TIMESTAMP, KEY_LEVEL, KEY_CONTENT, KEY_THREAD]

pattern_log_line = r'''
    (?P<timestamp>\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}(,\d{0,3})?)
    \s+
    (?P<level>ERROR|INFO|DEBUG)
    \s+
    (?P<file>\w+)
    :
    (?P<line>\d+)
    \s+
    (?P<content>.*)
    \s*
    \[(?P<thread>.+?)\]
'''

ENTRY_KEYS = re.findall(r'\?P<(\w+)>', pattern_log_line)
RE_LOG_LINE = re.compile(pattern_log_line, re.IGNORECASE|re.DOTALL|re.VERBOSE)

def parse_timestamp(timestamp):
    """Convert a string of the form 2011-03-10 15:10:34,687
      to a pandas TimeStamp
    """
    dt, ms = timestamp.split(',')
    return Timestamp(dt) + DateOffset(microseconds=int(ms)*1000)
    
    
if False:    
    dt = parse_timestamp('2013-04-22 02:40:43,123')
    print type(dt)
    print dt
    exit()

def decode_log_line(line):
    """ Split a PaperCut print-provider.log line into its parts. 
        See pattern_log_line for the parts
    """
    line = line.rstrip('\n').strip()
    m = RE_LOG_LINE.search(line)
    if not m:
        return None
    d = m.groupdict() 
    if False:
        try:  
            ts = d.get('timestamp', '')
            dateutil.parser.parse(d.get('timestamp', ''))
        except:
            print line
            print ts
            raise
            
    
    return [
        parse_timestamp(d.get('timestamp', '')),
        d.get('level'),
        d.get('file'),
        int(d.get('line', '-1')),
        d.get('content'),
        d.get('thread')
    ] 
    
def get_entries(log_file):
    """Generator that returns log entries for each line in log_file that matches
        RE_LOG_LINE
    """    
    with open(log_file, 'rt') as f:
        for i,line in enumerate(f):
            e = decode_log_line(line)
            if e:
                yield [log_file,i] + e
            
def log_file_to_df(log_file):
    # Why can't we pass a generator to DataFrame()?
    #entries = get_entries(log_file)
    entries = [e for e in get_entries(log_file)]
    assert all(len(e) == len(entries[0]) for e in entries)
    return DataFrame(entries, columns = ['logfile', 'logline'] + ENTRY_KEYS)

def make_timestamps_unique(df):
    """Make all the timestamps in DataFrame df unique by making each
        timestamp at least 1 us greater than timestamp of preceeding row
    """
    last = df.ix[0, 'timestamp']
    for i in range(1, len(df)):
        if df.ix[i,'timestamp'] <= last:
            df.ix[i,'timestamp'] = last + DateOffset(microseconds=1)
        last = df.ix[i,'timestamp']
    
df = log_file_to_df('server.log')
make_timestamps_unique(df)

print df
print df.head()
print df.iloc[0]

ts = df['timestamp']
last = ts[0]
for x in ts[1:]:
    assert x > last, '\n\t%s\n\t%s' % (last, x)
    last = x
    
print '-' * 80    
errors = df[df.level=='ERROR']
print errors
print errors.head()
print '-' * 40
print errors.irow(1)
    
print    
print df.timestamp.min()
print df.timestamp.max()    
print df.timestamp.max() - df.timestamp.min()
