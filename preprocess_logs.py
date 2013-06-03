# -*- coding: utf-8 -*-
"""
    Preprocess DataFrames created by load_logs.py
    
    load_logs.py stores a DataFrame in an HDF5 file called logs.py in a subdirectory
    of ObjectDirectory.ROOT which defaults to ./data
    
    preprocess.py reads this file and creates
    
    lfl_freq (lfl_freq.h5) Per-minute frequency counts of log entries over the time covered by the server logs for each file:line
    lfl_freq_corr (lfl_freq_corr.h5) Correlations of each file:line series in lfl_freq
    lfl_sorted (lfl_sorted.pkl) Lists of (level,file,line) <==> file:line string mappings
"""
from __future__ import division
import re, sys, glob 
import numpy as np
import pandas as pd
from pandas import DataFrame, Series, Timestamp, DateOffset, HDFStore
from datetime import datetime, timedelta
from collections import OrderedDict
from common import ObjectDirectory, versions


def truncate_to_minutes(timestamp):
    """Truncate a timestamp to the previous whole minute"""
    return Timestamp(datetime(timestamp.year, timestamp.month, timestamp.day, timestamp.hour, 
        timestamp.minute))


def get_details(df):
    return {
        'start_time': df.index.min(), 
        'end_time': df.index.max(),
        'count': df.count()
    }    
    
# 1 sample/minute for a day    
MAX_SAMPLES = 24 * 60    
    
def get_minute_counts(df, start_time, end_time):
    """Return a DataFrame whose index is 1 minute increments over the duration
        of df and whose values are the number of entries in df over each
        minute
    """
    print ' get_minute_counts %s %s %d' % (start_time, end_time, len(df)), 
    assert start_time < end_time
    duration = end_time - start_time
    period_min = max(1, duration.total_seconds()/60/MAX_SAMPLES)
    base_sec = int(period_min * 20.0)
    sample_sec = int(period_min * 60.0)
    print 'duration=%s,base_sec=%d,sample_sec=%d' % (duration, base_sec, sample_sec),
    
    bins = df.file.resample('%dS' % base_sec, how='count', convention='center')
    # Empty means zero counts !
    bins = bins.fillna(0.0)
    # Smooth by 2 bin widths
    bins = pd.rolling_mean(bins, 6, center=True)
    # Get the 1 minute bins
    bins = bins.resample('%dS' % sample_sec, how='sum', convention='center')  
    # Empty means zero counts !
    bins = bins.fillna(0.0)
    # All the NaNs should have been converted to zeros
    assert not pd.isnull(bins).any()
    # Remove the ends so data is full 1-minutes bins aligned on whole minutes 
    bins = bins[start_time: end_time]
    print '>>'
    return bins


def preprocess(directory, n_entries):

    hdf_path = directory.get_path('logs.h5', temp=False)
    print 'hdf_path: %s' % hdf_path

    store = HDFStore(hdf_path)
    print 'Keys: %s' % store.keys()
    print store
    store.close()
    df = pd.read_hdf(hdf_path, 'logs')

    #df = directory.load('logs.h5')
    print 'df: %s' % df

    if n_entries >= 0:
        df = df[:n_entries]

    secs = (df.index.max() - df.index.min()).total_seconds()
    hours = secs/3600
    levels = df.level.unique()

    print '%.1f hours of logs' % hours

    print '%d log entries/hour' % int(len(df)/hours)
    print '%.1f thousand log entries/hour' % (int(len(df)/hours)/1000.0)
    print df.shape, df.columns
    for level in levels:
        print '%-5s : %5d' % (level, len(df[df.level==level]))
    print 'df : %s' % str(df.shape)

    if False:        
        def get_peak(counts):
            """Retun the peak value in Series counts"""
            if len(counts) == 0:
                return None
            return counts.indmax()    
            # return counts.index[counts.argmax()]        

            
    start_time, end_time = df.index.min(), df.index.max()
    print 'orginal: start_time, end_time = %s, %s' % (start_time, end_time)

    # Start time and end time trunctated to whole minutes
    start_time = truncate_to_minutes(start_time + timedelta(minutes=2))
    end_time = truncate_to_minutes(end_time - timedelta(minutes=2))
    print 'cleaned: start_time, end_time = %s, %s' % (start_time, end_time)

    details = get_details(df)
    directory.save('details', details)
        
    # The counts for each 1 minute bin
    minute_counts = get_minute_counts(df, start_time, end_time)
    print 'minute_counts: %s\n%s' % (type(minute_counts), minute_counts.describe())    
    print 'total entries: %s' % minute_counts.sum()

    level_counts = {level: get_minute_counts(df[df.level==level], start_time, end_time)
            for level in levels}
 
    #level_peaks = {level: get_peak(level_counts[level])  for level in levels}  
    # print 'level_peaks: %s' % level_peaks     

    if False:
        unique_files = df.file.unique()
        print '%d source files' % len(unique_files)
        for i, fl in enumerate(sorted(unique_files)[:5]):
            print '%3d: %s' % (i, fl)

        directory.save('unique_files', unique_files)    
        
    #
    # Get all the unique log messages
    #
    level_file_line = df.groupby(['level', 'file', 'line'])
    lfl_size = level_file_line.size()
    lfl_sorted = lfl_size.order(ascending=False)
    print 'lfl_sorted: %s' % str(lfl_sorted.shape)

    #directory.save('level_file_line', tuple(level_file_line)) 
    directory.save('lfl_sorted', lfl_sorted)

    # file:line uniquely identifies each level,file,line
    # Construct mappings in both directions
    lfl_to_string = OrderedDict(((lvl,fl,ln), '%s:%d' % (fl,ln)) for lvl,fl,ln in lfl_sorted.index)
    string_to_lfl = OrderedDict(('%s:%d' % (fl,ln), (lvl,fl,ln)) for lvl,fl,ln in lfl_sorted.index)
    print 'string_to_lfl: %s' % len(string_to_lfl)

    # [((level,file,line),count)] sorted by count in descending order
    entry_types_list = zip(lfl_sorted.index, lfl_sorted)

    # {(level,file,line) : count}
    entry_types = OrderedDict(entry_types_list)
    directory.save('entry_types', entry_types)
    print 'entry_types: %s' % len(entry_types)

    #
    # Build the correlation table
    # 
    threshold = min(100, len(df)//1000)
    lfl_freq_dict = {
        s: get_minute_counts(df[(df.file==fl) & (df.line==ln)], start_time, end_time)
            for s,(lvl,fl,ln) in string_to_lfl.items()
                if len(df[(df.file==fl) & (df.line==ln)]) >= threshold
    }
    print '++++'
    lfl_freq = DataFrame(lfl_freq_dict, columns=string_to_lfl.keys())              
    directory.save('lfl_freq', lfl_freq)

    lfl_freq_corr = lfl_freq.corr()
    directory.save('lfl_freq_corr', lfl_freq_corr)
    print 'lfl_freq_corr: %s' % str(lfl_freq_corr.shape)


def main():
    import optparse

    parser = optparse.OptionParser('python %s [options]' % sys.argv[0])
    parser.add_option('-i', '--path', dest='path', default=None, 
            help='Saved log file directory')      
    parser.add_option('-n', '--number-entries', dest='n_entries', type='int', default=-1, 
            help='Number of log entries to process')         
    options, args = parser.parse_args()

    if not options.path:
        print '    Usage: %s' % parser.usage
        print __doc__
        print '    --help for more information'
        exit()

    directory = ObjectDirectory(options.path)
    print directory

    preprocess(directory, n_entries)

if __name__ == '__main__':
    versions()
    main()
 