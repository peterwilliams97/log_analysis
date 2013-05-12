# -*- coding: utf-8 -*-

from __future__ import division
import re, sys, glob 
import numpy as np
import pandas as pd
from pandas import DataFrame, Series, Timestamp, DateOffset
import matplotlib as mpl
from datetime import datetime, timedelta
from collections import OrderedDict
from common import ObjectDirectory


print 'python:', sys.version
print 'numpy:', np.__version__
print 'matplotlib:', mpl.__version__
print 'pandas:', pd.__version__
print '-' * 80


import optparse

parser = optparse.OptionParser('python %s [options]' % sys.argv[0])
parser.add_option('-i', '--path', dest='path', default=None, 
        help='Saved log file directory')            
options, args = parser.parse_args()

if not options.path:
    print '    Usage: %s' % parser.usage
    print __doc__
    print '    --help for more information'
    exit()
 


directory = ObjectDirectory(options.path)
print directory

df = directory.load('logs.h5')
print df

secs = (df.index.max() - df.index.min()).total_seconds()
hours = secs/3600
levels = df.level.unique()

print '%.1f hours of logs' % hours

print '%d log entries/hour' % int(len(df)/hours)
print '%.1f thousand log entries/hour' % (int(len(df)/hours)/1000.0)
print df.shape, df.columns
for level in levels:
    print '%-5s : %5d' % (level, len(df[df.level==level]))
print 'Total : %d' % df.shape[0] 


def truncate_to_minutes(timestamp):
    """Truncate a timestamp to the previous whole minute"""
    return Timestamp(datetime(timestamp.year, timestamp.month, timestamp.day, timestamp.hour, 
        timestamp.minute))

# Remove the ends so data is full 1-minutes bins aligned on whole minutes        
start_time, end_time = df.index.min(), df.index.max()
print 'start_time, end_time = %s, %s' % (start_time, end_time)
start_time = truncate_to_minutes(start_time + timedelta(minutes=2))
end_time = truncate_to_minutes(end_time - timedelta(minutes=2))
print 'start_time, end_time = %s, %s' % (start_time, end_time)

# !@#$ Why doesn't this work?
#df_whole_minutes = df.between_time(start_time, end_time)
#print 'df_whole_minutes = %s' % df_whole_minutes
df_whole_minutes = df
#df = df.between_time(start_time, start_time + timedelta(hours=2))
#print '**', df.index.min(), df.index.max()

def get_minute_counts(df, subsample=False):
    bars = df.file.resample('1S', how='count', convention='center')
    bars = pd.rolling_mean(bars, 120, center=True)
    return bars.resample('120S', how='sum', convention='center')   
    
    if subsample:
        bars = df.file.resample('1S', how='count', convention='center')
        bars = pd.rolling_sum(bars, 60, center=True)
    else:
        bars = df.file.resample('1min', how='count', convention='center')
    return bars
    

# The counts for each 1 minute bin
minute_counts = get_minute_counts(df)
minute_counts_ss = get_minute_counts(df, subsample=True)
print 'minute_counts: %s' % minute_counts.describe()    
print 'minute_counts_ss: %s' % minute_counts_ss.describe()

level_counts = {level: get_minute_counts(
        df[df.level==level], subsample=True)
        for level in levels}
     
 
def get_peak(counts):
    return counts.index[counts.argmax()]
   
        
level_peaks = {level: get_peak(level_counts[level])  for level in levels}       


unique_files = df.file.unique()
print '%d source files' % len(unique_files)
for i, fl in enumerate(sorted(unique_files)):
    print '%3d: %s' % (i, fl)

directory.save('unique_files', unique_files)    
    
#
# Get all the unique log messages
#
level_file_line = df.groupby(['level', 'file', 'line'])
lfl_size = level_file_line.size()
lfl_sorted = lfl_size.order(ascending=False)
print 'level, file, line: %s' % lfl_sorted.shape

#directory.save('level_file_line', tuple(level_file_line)) 
directory.save('lfl_sorted', lfl_sorted)

# file:line uniquely identifies each level,file,line
# Construct mappings in both directions
lfl_to_string = OrderedDict(((lvl,fl,ln), '%s:%d' % (fl,ln)) for lvl,fl,ln in lfl_sorted.index)
string_to_lfl = OrderedDict(('%s:%d' % (fl,ln), (lvl,fl,ln)) for lvl,fl,ln in lfl_sorted.index)

# [((level,file,line),count)] sorted by count in descending order
entry_types_list = zip(lfl_sorted.index, lfl_sorted)

# {(level,file,line) : count}
entry_types = OrderedDict(entry_types_list)
directory.save('entry_types', entry_types)

# Build the correlation table
 


lfl_freq_dict = {s: get_minute_counts(df[(df.file==fl) & (df.line==ln)])
              for s,(lvl,fl,ln) in string_to_lfl.items()}
lfl_freq = DataFrame(lfl_freq_dict, columns=string_to_lfl.keys())              
directory.save('lfl_freq', lfl_freq)

lfl_freq_corr = lfl_freq.corr()
directory.save('lfl_freq_corr', lfl_freq_corr)

if False:

    # How correlated are the frequencies of the log messages?
    # for lvl, fl, ln in entry_types.keys():
     
    NUM_ENTRY_TYPES = 1000

    lfl_rolling_sums = []
    for lvl, fl, ln in entry_types.keys()[:NUM_ENTRY_TYPES]:
        entries = df_whole_minutes[(df_whole_minutes.file==fl) & (df_whole_minutes.line==ln)]
        minute_counts = get_minute_counts(entries)
        lfl_rolling_sums.append(((lvl,fl,ln), minute_counts)) 

    # The uncorrelated log messages
    CORR_THRESH = 0.9

    uncorrelated = OrderedDict()
    key,rs = lfl_rolling_sums[0]
    uncorrelated[key] = rs, 0.0
    for key,rs in lfl_rolling_sums[1:]:
        corr = max(rs.corr(x) for x,_ in uncorrelated.values())
        if corr < CORR_THRESH:
            uncorrelated[key] = rs, corr

    uncorr_details = { (lvl,fl,ln) : (corr, df[(df.file==fl) & (df.line==ln)].file.count())
        for (lvl,fl,ln),(rs,corr) in uncorrelated.items()
    }
    print 'uncorr_details=%s' % uncorr_details

    uncorr_details_list = sorted(uncorr_details.items(), key=lambda x: -x[1][1])
    print 'uncorr_details_list' 
    for x in uncorr_details_list:
        print x
        


