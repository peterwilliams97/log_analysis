from __future__ import division
"""
    Decode PaperCut logs by reading them into a map

    Log lines look like
        2011-03-10 15:10:34,687 ERROR BaseXMLRPCServlet:110 - Error during XMLRPC request on: client-xmlrpc, IP: 10.203.0.122 [3751531@http-436]
        2011-03-10 15:10:34 INFO BaseXMLRPCServlet:110 - Error during XMLRPC request on: client-xmlrpc, IP: 10.203.0.122 [3751531@http-436]
        Jan 21 09:32:07 DEBUG:  insert_fragments_in_file: (subset 0) offsets={0:519:1,1:75992:0,} [3744]
        Jul 01 13:01:32 INFO : Registering printer with details: printserver.warren-wilson.edu\DesignandConstruction-Client, FMTS, net://10.0.1.143, Brother MFC-J6710DW


    Created on 25/3/2011

    @author: peter
"""
import re, sys, os, glob, logging, optparse, copy, time, datetime

KEY_TIMESTAMP = 'timestamp'
KEY_LEVEL = 'level'
KEY_CONTENT = 'content'
KEY_THREAD = 'thread'
ENTRY_KEYS = [KEY_TIMESTAMP, KEY_LEVEL, KEY_CONTENT, KEY_THREAD]
ENTRY_KEYS2 = [KEY_TIMESTAMP, KEY_LEVEL, KEY_CONTENT] 

pattern_log_line = r'(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(,\d{0,3}){0,1})\s+(?P<level>ERROR|INFO|DEBUG)\s+(?P<content>.*)\s\[(?P<thread>.+?)\]'
re_log_line = re.compile(pattern_log_line, re.IGNORECASE|re.DOTALL)

pattern_log_line2 = r'(?P<timestamp>\S{3} \d{2} \d{2}:\d{2}:\d{2}(,\d{0,3}){0,1})\s+(?P<level>ERROR|INFO|DEBUG):\s+(?P<content>.*)\s\[(?P<thread>.+?)\]'
re_log_line2 = re.compile(pattern_log_line2, re.IGNORECASE|re.DOTALL)

pattern_log_line3 = r'(?P<timestamp>\S{3} \d{2} \d{2}:\d{2}:\d{2}(,\d{0,3}){0,1})\s+(?P<level>ERROR|INFO|DEBUG)\s*:\s+(?P<content>.*)'
re_log_line3 = re.compile(pattern_log_line3, re.IGNORECASE|re.DOTALL)

def decode_log_line(log_line):
    """ Split a PaperCut print-provider.log line into its parts. 
        See pattern_log_line for the parts
        No parts => not a formatted log line
    """
    line = log_line.strip('\n').strip()
    m = re_log_line.search(line)
    result = {}
    entry_keys = ENTRY_KEYS
    if not m:
        m = re_log_line2.search(line)
        #print '!',
    if not m:
        m = re_log_line3.search(line)
        # No thread id in this pattern
        entry_keys = ENTRY_KEYS2        
    if m:
        for k in entry_keys:
            #print '%10s :' % k, m.group(k)
            result[k] = m.group(k)
    return result
    
def encode_log_entry(entry):
    return '%s %s %s [%s]' % (entry[KEY_TIMESTAMP], entry[KEY_LEVEL], entry[KEY_CONTENT], entry[KEY_THREAD])

#2011-03-10 15:10:34
pattern_timestamp = r'(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2}) (?P<hour>\d{2}):(?P<min>\d{2}):(?P<sec>\d{2})'
re_timestamp = re.compile(pattern_timestamp)

#Jul 01 13:00:31
pattern_timestamp2 = r'(?P<mon>\S{3}) (?P<day>\d{2}) (?P<hour>\d{2}):(?P<min>\d{2}):(?P<sec>\d{2})'
re_timestamp2 = re.compile(pattern_timestamp2)
MONTHS = {'jan':1, 'feb':2, 'mar':3, 'apr':4, 'may':5, 
          'jun':6, 'jul':7, 'aug':8, 'sep':9, 'oct':10, 'nov':11, 'dec':12 }

def get_datetime(timestamp):
    m = re_timestamp.search(timestamp)
    if m:
        d = datetime.date(int(m.group('year')), int(m.group('month')), int(m.group('day')))
        t = datetime.time(int(m.group('hour')), int(m.group('min')), int(m.group('sec')))
    else:
        m = re_timestamp2.search(timestamp)
        d = datetime.date(2011, MONTHS[m.group('mon').lower()], int(m.group('day')))
        t = datetime.time(int(m.group('hour')), int(m.group('min')), int(m.group('sec')))
    if m:
        dt = datetime.datetime.combine(d, t)
        return dt
    return None	

if False:
    log_lines = ['Jan 21 09:32:07 DEBUG:  insert_fragments_in_file: (subset 0) offsets={0:519:1,1:75992:0,} [3744]',
        '2011-03-10 15:10:34,687 ERROR BaseXMLRPCServlet:110 - Error during XMLRPC request on: client-xmlrpc, IP: 10.203.0.122 [3751531@http-436]',
        '2011-03-10 15:10:34 INFO BaseXMLRPCServlet:110 - Error during XMLRPC request on: client-xmlrpc, IP: 10.203.0.122 [3751531@http-436]']
    for line in log_lines:
        result = decode_log_line(line)
        print '-' * 50
        print line
        print result
    exit()

def read_log_gen(f):
    """ Generator to read a line of data from PaperCut log file """
    while True:
        line = f.readline()
        if not line:
            break
        yield line.rstrip('\n').strip()    

def _read_log_lines(log_file):
    f = open(log_file, 'rt')
    if not f:
        print 'Could not open', log_file
        return None,
    log_lines = []
    for line in read_log_gen(f):
        log_lines.append(line)
    f.close() 
    return log_lines
    
_verbose = False

# !@#$ Should decode lines in cache!!!    
class LogReaderCache:
    def __init__(self):
        self._cache = {}
        self._calls = 0
        self._misses = 0
    
    def get(self, log_file):
        self._calls += 1
        if not self._cache.has_key(log_file):
            log_lines = _read_log_lines(log_file)
            if log_lines:
                #print 'cache load: %s => %d lines' % (log_file, len(log_lines))
                self._cache[log_file] = log_lines
                self._misses += 1
        self.show_stats()        
        return self._cache[log_file]
        
    def show_stats(self):
        if _verbose:
            print 'Cache: calls=%d, misses=%d, hits=%d' % (self._calls, self._misses, self._calls-self._misses)
   
# The global log reader cache.   
_log_reader_cache = LogReaderCache()  

def read_log_lines(log_file):
    global _log_reader_cache
    return _log_reader_cache.get(log_file)
   
class LogDecoderCache:
    def __init__(self):
        self._cache = {}
        self._calls = 0
        self._misses = 0
    
    def get(self, log_file):
        self._calls += 1
        if not self._cache.has_key(log_file):
            log_lines = read_log_lines(log_file)
            log_entries = [decode_log_line(line) for line in log_lines]
            log_entries = [x for x in log_entries if x.has_key(KEY_LEVEL)] 
            if log_entries:
                #print 'cache load: %s => %d lines' % (log_file, len(log_lines))
                self._cache[log_file] = log_entries
                self._misses += 1
        self.show_stats()        
        return self._cache[log_file]
       
    def show_stats(self):
        if _verbose:
            print 'Cache: calls=%d, misses=%d, hits=%d' % (self._calls, self._misses, self._calls-self._misses)   

# The global log decoder cache.   
_log_decoder_cache = LogDecoderCache()  

def get_decoded_log_lines(log_file):
    global _log_decoder_cache
    return _log_decoder_cache.get(log_file)

def get_timedelta_seconds(td):
    return (td.microseconds + (td.seconds + td.days * 24 * 3600) * 10**6) / 10**6 

def get_entry_datetime(entry):
    if not entry.has_key(KEY_TIMESTAMP):
        return None
    return get_datetime(entry[KEY_TIMESTAMP])	

def get_time_gaps(log_entries):
    time_gaps = []
    dt0 = None 
    for entry in log_entries:
        dt = get_entry_datetime(entry)
        if dt is None or dt0 is None:
            time_gaps.append(None)
        else: 
            delta = dt - dt0
            time_gaps.append(get_timedelta_seconds(delta))
        if dt is not None:
            dt0 = dt
    if False:
        print 'len(log_entries):', 	len(log_entries)	
        print 'len(time_gaps):', len(time_gaps)	
        for i in range(len(log_entries)):
            print get_entry_datetime(log_entries[i]), time_gaps[i]

    return time_gaps

def get_time_gap_matches(log_entries, min_time_gap):
    #print 'get_time_gap_matches', len(log_entries), min_time_gap
    time_gaps = get_time_gaps(log_entries)
    assert(len(time_gaps) == len(log_entries))
    if min_time_gap <= 0:
        return  [True for i in range(len(log_entries))]
    #print '$$$$$$'	
    matches = [False for i in range(len(log_entries))]
    for i in range(len(log_entries)):
        if time_gaps[i] is not None and time_gaps[i] > min_time_gap:
            matches[i] = True
            for j in range(i-1, 0, -1):
                if time_gaps[j] is not None:
                    break
                print j, '!!'
                matches[j] = True
    if False:
        for i in range(len(log_entries)):
            print get_entry_datetime(log_entries[i]), time_gaps[i], matches[i]	
        exit()
    return matches

# !@#$ Inefficient. 
#   Discard the non logs from cache	??
def get_first_log_entry(log_file):
    """ Return the first log entry in potential log file log_file or None if there """
    log_lines = read_log_lines(log_file)
    for line in log_lines:
        entry = decode_log_line(line)
        if entry.has_key(KEY_LEVEL) and entry.has_key(KEY_CONTENT):
            return get_entry_datetime(entry)
    return None	

def get_count(a_list):
    return sum([1 if x else 0 for x in a_list])
   
_VERSION_IDS = ['Starting the print provider service', 'CUPS Provider Version:', 
    'Starting application server version:'] 
def show_version(log_file):
    log_lines = read_log_lines(log_file)
    for line in log_lines:    
        for id in _VERSION_IDS:
            if line.find(id) >= 0:
                print line

def show_sorted_log(log_file):
    """ Sort the log file lines by severity then date """
    log_entries = get_decoded_log_lines(log_file)
    SEVERITY = {'ERROR':0, 'INFO':1, 'DEBUG':2, 'DEV':3}
    if True:
        print log_entries[0]
        log_entries.sort(key = lambda x: (SEVERITY[x[KEY_LEVEL]], x[KEY_TIMESTAMP]))
        for x in log_entries:
            print x[KEY_LEVEL], x[KEY_TIMESTAMP], x[KEY_CONTENT]
        exit()
    
def show_matches(log_file, levels_to_match, content_to_match, content_to_not_match, thread_id, lines_before, lines_after, 
                match_plain, min_time_gap, first_time, last_time): 
    """ Decode a log file """
    if _verbose:
        print 'show_matches:', 'levels_to_match=', levels_to_match, 'content_to_match=', content_to_match, 'content_to_not_match=', content_to_not_match 
        print ' ' * 4, 'first_time:',first_time, ',last_time:', last_time
        print ' ' * 4, 'thread_id=', thread_id, 'lines_before=', lines_before, ' lines_after=',  lines_after

    log_entries = get_decoded_log_lines(log_file) 
    #log_lines = read_log_lines(log_file)
    #if not log_lines:
    #    return
    #  [decode_log_line(line) for line in log_lines]
    # log_entries = [x for x in log_entries if x.has_key(KEY_LEVEL)]
    
    time_gaps = get_time_gaps(log_entries)
    num_lines = len(log_entries)
    if _verbose:
        print 'log_entries=', log_entries
        
    def is_match(entry):
        #print entry
        if not entry.has_key(KEY_LEVEL) or not entry.has_key(KEY_CONTENT) or not entry.has_key(KEY_THREAD):
            return False
        match = True
        if not entry[KEY_LEVEL] in levels_to_match:
            match = False
        if thread_id:
            if thread_id.lower() not in entry[KEY_THREAD].lower():
                match = False	
        if content_to_match:
            if content_to_match.lower() not in entry[KEY_CONTENT].lower():
                match = False
        if content_to_not_match:
            if content_to_not_match.lower() in entry[KEY_CONTENT].lower():
                match = False
        dt = get_entry_datetime(entry)
        if get_timedelta_seconds(dt - first_time) < 0:
            #print dt, first_time, '**'
            match = False
        if get_timedelta_seconds(dt - last_time) > 0:
            #print dt, last_time
            match = False
        return match
                
    level_matches = [is_match(entry) for entry in log_entries]
    time_gap_matches = get_time_gap_matches(log_entries, min_time_gap)
    matches = [level_matches[i] and time_gap_matches[i] for i in range(num_lines)]
    matches_tol = [any(matches[i-lines_after:i+lines_before+1]) for i in range(num_lines)]

    if _verbose:
        print '   level_matches:', get_count(level_matches)
        print 'time_gap_matches:', get_count(time_gap_matches)
        print '         matches:', get_count(matches)
        print '     matches_tol:', get_count(matches_tol)
        
    matches_found = False    
    for i in range(num_lines):
        matched_plain = match_plain and len(log_entries[i]) == 0
        if matches_tol[i] or matched_plain:
            if len(log_entries[i]) > 0:
                print encode_log_entry(log_entries[i])
            matches_found = True

    if not matches_found:
        print 'No matches found for', levels_to_match, content_to_match,  'in',  log_file       

if __name__ == '__main__':

    parser = optparse.OptionParser('usage: python ' + sys.argv[0] + ' [options] <input file>')
    parser.add_option('-E', '--error', action='store_true', dest='show_error', default=False, help='show ERROR lines')
    parser.add_option('-I', '--info', action='store_true', dest='show_info', default=False, help='show INFO lines')
    parser.add_option('-D', '--debug', action='store_true', dest='show_debug', default=False, help='show DEBUG lines')
    parser.add_option('-i', '--include', dest='match_text', default=None, help='print all lines matching this text')
    parser.add_option('-e', '--exclude', dest='unmatch_text', default=None, help='print all lines not matching this text')
    parser.add_option('-t', '--thread-id', dest='thread_id', default=None, help='print all lines with this thread id')
    parser.add_option('-b', '--lines-before', dest='lines_before', default='0', help='number of lines before matched line to show')
    parser.add_option('-a', '--lines-after', dest='lines_after', default='0', help='number of lines after matched line to show')
    parser.add_option('-p', '--match-plain', action='store_true', dest='match_plain', default=False, help='match plain (non-log) lines')
    parser.add_option('-g', '--time-gap', dest='min_time_gap', default='0', help='minimum time in seconds between log entries')
    parser.add_option('-f', '--first-time', dest='first_time', default='1900-01-01 00:00:00', help='first log time')
    parser.add_option('-l', '--last-time', dest='last_time', default='2099-01-01 00:00:00', help='last log time')
    parser.add_option('-v', '--version', action='store_true', dest='show_version', default=False, help='show print provider version')
    parser.add_option('-s', '--sort', action='store_true', dest='sort_file', default=False, help='sort log file by severity')
         
    (options, args) = parser.parse_args()
    if len(args) < 1:
        print parser.usage
        print 'options:', options
        print 'args', args
        print 'At least one file name and one command switch must be supplied'
        print '--help option for more informaton'
        exit()
        
    print 'options:', options
    # log_file is log file. It is read as text. 
    log_file_mask = args[0]
    log_file_list_raw = [f for f in glob.glob(log_file_mask) if not os.path.isdir(f)]
    log_file_list_raw2 = [f for f in log_file_list_raw if get_first_log_entry(f) is not None]
    log_file_list = sorted(log_file_list_raw2, key=lambda x: get_first_log_entry(x))
    print 'log_file_mask =', log_file_mask
    print 'log_file_list =', log_file_list
    if not log_file_list:
        print 'log_file_list_raw =', log_file_list_raw

    if False:
        # Show sort order
        for log_file in log_file_list:
            print get_first_log_entry(log_file), log_file 

    # Show version
    if options.show_version:
        for log_file in log_file_list:
            show_version(log_file)
            exit()
            
    if options.sort_file:        
        show_sorted_log(log_file)
        exit()    

    # By default match on all log levels 
    levels_to_match = ['ERROR', 'INFO', 'DEBUG']

    # Handle specific requests for matching on log levels
    if options.show_error or options.show_info or options.show_debug:
        levels_to_match = []
        if options.show_error:
            levels_to_match.append('ERROR') 
        if options.show_info:
            levels_to_match.append('INFO') 
        if options.show_debug:
            levels_to_match.append('DEBUG')  

    for log_file in log_file_list:
        print log_file, '-' * 40
        show_matches(log_file, levels_to_match, options.match_text, options.unmatch_text, options.thread_id, 
            int(options.lines_before), int(options.lines_after), options.match_plain, int(options.min_time_gap),
            get_datetime(options.first_time), get_datetime(options.last_time))        

