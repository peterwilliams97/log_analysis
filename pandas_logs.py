"""
Decode PaperCut logs

Log lines look like
	2011-03-10 15:10:34,687 ERROR BaseXMLRPCServlet:110 - Error during XMLRPC request on: client-xmlrpc, IP: 10.203.0.122 [3751531@http-436]
	2011-03-10 15:10:34 INFO BaseXMLRPCServlet:110 - Error during XMLRPC request on: client-xmlrpc, IP: 10.203.0.122 [3751531@http-436]
	 Jan 21 09:32:07 DEBUG:  insert_fragments_in_file: (subset 0) offsets={0:519:1,1:75992:0,} [3744]

Created on 24/3/2011

@author: peter
"""
import re, sys, os, glob, logging, optparse, copy, time
import dateutil.parser
import numpy as np
import pandas as pd
from pandas import DataFrame, Series, Timestamp
from datetime import datetime, timedelta

KEY_TIMESTAMP = 'timestamp'
KEY_LEVEL = 'level'
KEY_CONTENT = 'content'
KEY_THREAD = 'thread'
ENTRY_KEYS = [KEY_TIMESTAMP, KEY_LEVEL, KEY_CONTENT, KEY_THREAD]

pattern_log_line = r'(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(,\d{0,3}){0,1})\s+(?P<level>ERROR|INFO|DEBUG)\s+(?P<content>.*)\s\[(?P<thread>.+?)\]'
RE_LOG_LINE = re.compile(pattern_log_line, re.IGNORECASE|re.DOTALL)

def parse_timestamp(timestamp):
    parts = timestamp.split(',')
    dt = dateutil.parser.parse(parts[0])
    if len(parts) > 1:
        ms = int(parts[1])
        dt += timedelta(milliseconds=ms)
    return dt

def parse_timestamp(timestamp):
    parts = timestamp.split(',')
    dt = Timestamp(parts[0])
    if len(parts) > 1:
        ms = int(parts[1])
        delta = pd.DateOffset(microseconds=ms*1000)
        #np.timedelta64(ms, 'ms')
        dt += delta
    return dt
    
    
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
        d.get('content'),
        d.get('thread')
    ]    
    
    # m.groupdict().get(k) for k in ENTRY_KEYS]
    
def get_entries(log_file):
    with open(log_file, 'rt') as f:
        for line in f:
            e = decode_log_line(line)
            if e:
                yield e
            
def log_file_to_df(log_file):
    entries = [e for e in get_entries(log_file) if e]
    assert all(len(e) == len(entries[0]) for e in entries)
    return DataFrame(entries, columns = ENTRY_KEYS)
            
df = log_file_to_df('server.log')
print df
print df.head()
print df.iloc[0]

# Make timestamps unique
last = datetime(year=1900,month=1,day=1)
for i in range(len(df)):
    #print df.iloc[i]
    if df.ix[i,'timestamp'] <= last:
        df.ix[i,'timestamp'] = last + pd.DateOffset(microseconds=1)
        print df.ix[i, 'timestamp']
    last = df.ix[i,'timestamp']

ts = df['timestamp']
last = datetime(year=1900,month=1,day=1)
for x in ts:
    assert x > last, '\n\t%s\n\t%s' % (last, x)
    last = x
    
print '-' * 80    
errors = df[df.level=='ERROR']
print errors
print errors.head()
print '-' * 40
print errors.irow(1)
    