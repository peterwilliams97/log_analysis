# -*- coding: utf-8 -*-
"""
    Load PaperCut logs into a pandas DataFrame and store it in an HDF5 file.

    Tested with pandas 0.11.0 or higher.
    
    e.g. 
        python load_logs.py -s -o out_dir -i in_dir\server.log*
    
        - parses all the server_log* files in in_dir
        - converts them to a DataFrame
        - saves the DataFrame in the HDF5 file data/out_dir/logs.h5 in table '/logs'
        
        This is done in 2 steps
        
        1. Each server log is converted to an DataFrame and saved as a table in 
            data/out_dir/temp/progress.h5
        2. The HDF5 files in data/out_dir/temp are combined and saved as data/out_dir/logs.h5
        
        This is done to allow restarting during the processing of very large collections of 
        server.log files.

    FIXME:    
        Replace table '/logs' with '/table'        
    
"""
from __future__ import division
import re, sys, glob, os, time 
import numpy as np
import pandas as pd
from pandas import DataFrame, Series, Timestamp, DateOffset, HDFStore
from common import ObjectDirectory, versions


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

ENTRY_KEYS_SIMPLE = ENTRY_KEYS[:4]

def decode_log_line_simple(line):
    """ Return a parial list of the parts of a server.log line
        - date
        - time
        - level
        - file
        - line
    """
    parts = line[:100].split()[:4]
    if len(parts) < 4 or all(parts[2] != x for x in ('ERROR', 'INFO', 'DEBUG')):
        return None
    fl, ln = parts[3].split(':')
    return [
        parse_timestamp(' '.join(parts[:2])),
        parts[2],
        fl,
        int(ln)
    ]    

    
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


def get_header_entries(log_file, extra):
    """ Returns decoded log entries for all well-formed log entries in a log file"""
    
    decoder = decode_log_line if extra else decode_log_line_simple
    
    entries = []
    header = []
    in_header = True
    with open(log_file, 'rt') as f:
        for i, line in enumerate(f):
            line = line.rstrip('\n')
            entry = decoder(line)
            if entry:
                entries.append(entry)
            elif in_header:
                if i < 10 and line.startswith('#'):
                    header.append(line)
                else:
                    in_header = False
    return header, entries                
           
    
def log_file_to_df(log_file, extra):
    """Returns a pandas dataframe whose rows are the decoded entries of the lines in log_file"""
    # Why can't we construct a DataFrame with a generator?
    header, entries = get_header_entries(log_file, extra)
    entry_keys = ENTRY_KEYS if extra else ENTRY_KEYS_SIMPLE
    for e in entries:
        assert len(e) == len(entry_keys), '\n%s\n%s'  % (e, entry_keys)
    df = DataFrame(entries, columns=entry_keys)
    del entries
    return header, df


USEC = DateOffset(microseconds=1)

def make_timestamps_unique(df):
    """Make all the timestamps in DataFrame df unique by making each
        timestamp at least 1 �sec greater than timestamp of preceeding row
    """
    for i in range(1, len(df)):
        if df.ix[i,'timestamp'] <= df.ix[i-1,'timestamp']:
            df.ix[i,'timestamp'] = df.ix[i-1,'timestamp'] + USEC


def load_log(log_path, extra):
    """Return a pandas DataFrame for all the valid log entry lines in log_file
        The index of the DataFrame are the uniqufied timestamps of the log entries
    """
    header, df = log_file_to_df(log_path, extra)
    make_timestamps_unique(df)
    df = df.set_index('timestamp')
    return header, df


class LogSaver:
    """
        self.directory : Directory structure for temp and saved files
        self.log_list : List of server.log files to process
        self.extra : True if log messages and thread ids are to be saved too
        self.history_path : History of server.log conversions saved here
        self.progress_store_path : HDF5 file that holds one DataFrame for each server.log file 
        self.store_path : Final DataFrame of all server.log entries saved here
        self.history : History of server.log conversions
    """

    FINAL = 'logs'
    PROGRESS = 'progress'
    HISTORY = 'history'

    @staticmethod
    def normalize(name):
        return name.replace('\\', '_').replace('.', '_').replace('-', '_')
     
    @staticmethod
    def make_name(base_name, extra):
        if extra:
            return base_name + '.extra'
        else:
            return base_name
     
    #@staticmethod
    #def temp_name(log_list, extra):
    #    hsh = hash(log_list)
    #    sgn = 'n' if hsh < 0 else 'p'
    #    temp = 'temp_%s%08X' % (sgn, abs(hsh))
    #    return LogSaver.make_name(temp, extra)    

    def __init__(self, store_path, log_list, extra):
        self.directory = ObjectDirectory(store_path)
        self.log_list = tuple(sorted(log_list))
        self.extra = extra
        
        self.temp = LogSaver.temp_name(self.log_list, extra)
        self.history_path = self.directory.get_path(LogSaver.HISTORY, temp=True)
        self.progress_store_path = self.directory.get_path(LogSaver.PROGRESS, temp=True, is_df=True)
        self.store_path = self.directory.get_path(LogSaver.make_name(LogSaver.FINAL, extra), 
                            is_df=True)
        self.history = ObjectDirectory.load_object(self.history_path, {})
        
    def __repr__(self):
        return '\n'.join('%s: %s' % (k,v) for k,v in self.__dict__.items())
        
    def __str__(self):
        return '\n'.join([repr(self), '%d log files' % len(self.log_list)])    

    def save_all_logs(self, force=False):
         
        if os.path.exists(self.store_path):
            return
        if not force:
            assert not os.path.exists(self.history_path), '''
                %s exists but %s does not.
                There appears to be a conversion in progress.
                -f forces conversion to complete.
            ''' % (self.history_path, self.store_path)
        
        self.directory.make_dir_if_necessary(self.progress_store_path)
        self.progress_store = HDFStore(self.progress_store_path)
        for path in self.log_list:
            self.save_log(path)
        
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
        
        # Save the history in a corresponding file
        self.directory.save('history', self.history)
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
        header, df = load_log(path, extra=self.extra)
        self.progress_store.put(LogSaver.normalize(path), df)
        load_time = time.time() - start
        
        self.history[path] = {
            'start': df.index[0],
            'end': df.index[-1],
            'load_time': int(load_time),
            'num': len(df),
            'header': header
        }
        ObjectDirectory.save_object(self.history_path, self.history)
        del df
        print { k:v for k,v in self.history[path].items() if k != 'header' },
        print '%d of %d' % (len(self.history), len(self.log_list))

    def check(self):
        history = ObjectDirectory.load_object(self.history_path, {})
        sorted_keys = history.keys()
        sorted_keys.sort(key=lambda k: history[k]['start'])
        print '-' * 80
        print 'Time range by log file'
        for i, path in enumerate(sorted_keys):
            hist = history[path]
            print '%2d: %s  ---  %s : %s' % (i, hist['start'], hist['end'], path)
        
        path0 = sorted_keys[0]
        for path1 in sorted_keys[1:]:
            hist0,hist1 = history[path0],history[path1] 
            assert hist0['end'] < hist1['start'], '\n%s %s\n%s %s' % (
                path0, hist0, 
                path1, hist1)    
 

def load_log_pattern(hdf_path, path_pattern, force=False, clean=False, extra=False, 
                     number_files=-1):  

    path_list = glob.glob(path_pattern) 
    print path_list
    if not path_list:
        return

    if number_files >= 0:
        path_list = path_list[:number_files]

    log_saver = LogSaver(hdf_path, path_list, extra=extra)
    print
    print log_saver
    print
    if clean:
        print 'Cleaning temp files'
        log_saver.cleanup()
    log_saver.save_all_logs(force=force)


def main():
    import optparse
    
    parser = optparse.OptionParser('python %s [options]' % sys.argv[0])
    parser.add_option('-o', '--name', dest='hdf_path', default=None, 
            help='Name of the HDF5 file the DataFrame will be stored in')
    parser.add_option('-i', '--log-file', dest='path_pattern', default=None, 
            help='Log files to match')            
    parser.add_option('-f', '--force', dest='force', action='store_true', default=False, 
            help='''Force rebuilding of HDF5 file. 
        Rebuild the HDF5 file from the in-progress (temp) files and over-write the existing final 
        HDF5 file.''')
    parser.add_option('-c', '--clean', dest='clean', action='store_true', default=False, 
            help='Delete the in-progress (temp) files for this processing session.')        
    parser.add_option('-e', '--extra', dest='extra', action='store_true', default=False, 
            help='Extra information mode. Stores log content and thread id')
    parser.add_option('-n', '--number-files', dest='number_files', type='int', default=-1, 
            help='Max number of log files to process')            
    options, args = parser.parse_args()

    if not options.hdf_path or not options.path_pattern:
        print '    Usage: %s' % parser.usage
        print __doc__
        print '    --help for more information'
        exit()
 
    load_log_pattern(options.hdf_path, options.path_pattern, force=options.force,
            clean=options.clean, extra=options.extra, number_files=options.number_files)


if __name__ == '__main__':
    versions()  
    main()