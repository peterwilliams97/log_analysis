# -*- coding: utf-8 -*-

from __future__ import division
import re, sys, glob 
import pandas as pd
from pandas import DataFrame, Series, Timestamp, DateOffset
import matplotlib as mpl
from datetime import datetime, timedelta
from collections import OrderedDict

WIDTH = 24
mpl.rcParams['axes.color_cycle'] = ['b', 'r', 'c', 'y', 'k', 'm']
mpl.rcParams['legend.loc'] = 'best'
mpl.rcParams['figure.figsize'] = [WIDTH, 4]
np.set_printoptions(linewidth=200)
pd.set_printoptions(max_rows=100, max_columns=10, precision=4)

print 'python:', sys.version
print 'numpy:', np.__version__
print 'matplotlib:', mpl.__version__
print 'pandas:', pd.__version__

h5_p

df = pd.read_hdf(h5_path, 'logs')
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
start_time = truncate_to_minutes(start_time + timedelta(minutes=2))
end_time = truncate_to_minutes(end_time - timedelta(minutes=2))
print 'start_time, end_time = %s, %s' % (start_time, end_time)

df_whole_minutes = df.between_time(start_time, end_time)
print 'df_whole_minutes = %s' % df_whole_minutes

def get_minute_counts(df, subsample=False): 
    if subsample:
        bars = df.file.resample('1S', how='count', convention='center')
        bars = pd.rolling_sum(bars, 60, center=True)
    else:
        bars = df.file.resample('1min', how='count', convention='center')
    return bars
    

# The counts for each 1 minute bin
minute_counts = get_minute_counts(df_whole_minutes)
minute_counts_ss = get_minute_counts(df_whole_minutes, subsample=True)
print 'minute_counts: %s' % minute_counts.describe()    
print 'minute_counts_ss: %s' % minute_counts_ss.describe()

level_counts = {level: get_minute_counts(
        df_whole_minutes[df_whole_minutes.level=level], subsample=True)
        for level in levels}
        
 
def get_peak(counts):
    return = counts.index[counts.argmax()]
   
        
level_peaks = {level: get_peak(level_counts[level]
               for level in levels}       


unique_files = df.file.unique()
print '%d source files' % len(unique_files)
for i, fl in sorted(unique_files):
    print '%3d: %s' % (i, fl)

#
# Get all the unique log messages
#
level_file_line = df.groupby(['level', 'file', 'line'])
lfl_size = level_file_line.size()
lfl_sorted = lfl_size.order(ascending=False)
print 'level, file, line: %s' % lfl_sorted.shape

# [((level,file,line),count)] souted by count descending
entry_types_list = zip(lfl_sorted.index, lfl_sorted)

# {(level,file,line):count}
entry_types = OrderedDict(entry_types_list)

# How correlated are the frequencies of the log messages?
# for lvl, fl, ln in entry_types.keys():
 
NUM_ENTRY_TYPES = 100

lfl_rolling_sums = []
for lvl, fl, ln in entry_types.keys()[:NUM_FL]:
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
print 'uncorr_details_list=%s' % uncorr_details_list


