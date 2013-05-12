txt='2011-03-10 15:10:34,687 ERROR BaseXMLRPCServlet:110 - Error during XMLRPC request on: client-xmlrpc, IP: 10.203.0.122 [3751531@http-436]'

import re

txt1='2011-03-10 15:10:34,687 ERROR BaseXMLRPCServlet:110 - Error during XMLRPC request on: client-xmlrpc, IP: 10.203.0.122 [3751531@http-436]'
txt2='2011-03-10 15:10:34 INFO BaseXMLRPCServlet:110 - Error during XMLRPC request on: client-xmlrpc, IP: 10.203.0.122 [3751531@http-436]'

text = [txt1, txt2]

re_ts = r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(,\d{0,3}){0,1})'	# Time Stamp 1
re_ts = r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(,\d{0,3}){0,1})\s+(ERROR|INFO|DEBUG)\s+(.*)\s\[(.+?@.+?)\]'
re_ts = r'(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(,\d{0,3}){0,1})\s+(?P<level>ERROR|INFO|DEBUG)\s+(?P<content>.*)\s\[(?P<thread>.+?@.+?)\]'
rg_ts = re.compile(re_ts, re.IGNORECASE|re.DOTALL)

re1= '.*?'	# Non-greedy match on filler
re2= r'\[(.+?@.+?)\]'	# Square Braces 1
rg = re.compile(re1 + re2, re.IGNORECASE|re.DOTALL)

for txt in text:
	print '-' * 40
	print 'txt:', txt

	m = rg_ts.search(txt)
	if m:
		print m.groups()
		if False:
			timestamp = m.group(1)
			print 'timestamp:', timestamp
		for w in ['timestamp', 'level', 'content', 'thread']:
			print '%10s :' % w, m.group(w)

	if False:
		m = rg.search(txt)
		if m:
			thread_id = m.group(1)
			print 'thread_id:', thread_id

