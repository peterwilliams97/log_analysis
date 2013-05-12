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

KEY_TIMESTAMP = 'timestamp'
KEY_LEVEL = 'level'
KEY_CONTENT = 'content'
KEY_THREAD = 'thread'
ENTRY_KEYS = [KEY_TIMESTAMP, KEY_LEVEL, KEY_CONTENT, KEY_THREAD]

pattern_log_line = r'(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(,\d{0,3}){0,1})\s+(?P<level>ERROR|INFO|DEBUG)\s+(?P<content>.*)\s\[(?P<thread>.+?)\]'
re_log_line = re.compile(pattern_log_line, re.IGNORECASE|re.DOTALL)

def decode_log_line(log_line):
	""" Split a PaperCut print-provider.log line into its parts. 
		See pattern_log_line for the parts
	"""
	line = log_line.strip('\n').strip()
	m = re_log_line.search(line)
	result = {}
	if m:
		for k in ENTRY_KEYS:
			#print '%10s :' % k, m.group(k)
			result[k] = m.group(k)
	return result
	
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
		#print '!!!', entry
		if not line:
			#print '@@@'
			break
		yield line.rstrip('\n').strip()    

def show_matches(log_file, levels_to_match, content_to_match, content_to_not_match, thread_id): 
	""" Show all occurences of match_text in log_files """
	print 'show_matches:', 'levels_to_match=', levels_to_match, 'content_to_match=', content_to_match, 'content_to_not_match=', content_to_not_match, 'thread_id=', thread_id

	f = open(log_file, 'rt')
	if not f:
		print 'Could not open', log_file
		return
	
	def is_match(entry):
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
		return match
				
	matches_found = False    
	for line in read_log_gen(f):
		entry = decode_log_line(line)
		if is_match(entry):
			matches_found = True
			# print [entry[k] for k in ENTRY_KEYS]
			print line
			
	if not matches_found:
		print 'No matches found for', levels_to_match, content_to_match,  'in',  log_file
	f.close()        

if __name__ == '__main__':

	parser = optparse.OptionParser('usage: python ' + sys.argv[0] + ' [options] <input file>')
	parser.add_option('-o', '--output', dest='output_dir', default='output', help='output directory')
	parser.add_option('-E', '--error', action='store_true', dest='show_error', default=False, help='show ERROR lines')
	parser.add_option('-I', '--info', action='store_true', dest='show_info', default=False, help='show INFO lines')
	parser.add_option('-D', '--debug', action='store_true', dest='show_debug', default=False, help='show DEBUG lines')
	parser.add_option('-i', '--include', dest='match_text', default=None, help='print all lines matching this text')
	parser.add_option('-e', '--exclude', dest='unmatch_text', default=None, help='print all lines not matching this text')
	parser.add_option('-t', '--thread-id', dest='thread_id', default=None, help='print all lines with this thread id')
		 
	(options, args) = parser.parse_args()
	if len(args) < 1:
		print parser.usage
		print 'options:', options
		print 'args', args
		print 'At least one file name and one command switch must be supplied'
		print '--help option for more informaton'
		exit()

	# log_file is log file. It is read as text. 
	log_file_mask = args[0]
	log_file_list = sorted(f for f in glob.glob(log_file_mask) if not os.path.isdir(f))
	print 'log_file_mask =', log_file_mask
	print 'log_file_list =', log_file_list

	# By default match on all log levels 
	levels_to_match = ['ERROR', 'INFO', 'DEBUG']

	# Handle specific requests for matching on log levels
	if options.show_error or options.show_info or  options.show_debug:
		levels_to_match = []
		if options.show_error:
			levels_to_match.append('ERROR') 
		if options.show_info:
			levels_to_match.append('INFO') 
		if options.show_debug:
			levels_to_match.append('DEBUG')  
	
	for log_file in log_file_list:
		print log_file, '-' * 40
		show_matches(log_file, levels_to_match, options.match_text, options.unmatch_text, options.thread_id)        

  
    
    