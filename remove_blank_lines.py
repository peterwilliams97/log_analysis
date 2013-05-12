"""
Remove blank lines from a text file

Created on 25/3/2011

@author: peter
"""
import re, sys, os, glob, logging, optparse, copy, time

def read_log_gen(f):
	""" Generator to read a line of data from a text file """
	while True:
		line = f.readline()
		if not line:
			break
		yield line.rstrip('\n').strip()    

def show_non_blank_lines(log_file): 
	""" Show all non-blank lines in log file """

	f = open(log_file, 'rt')
	if not f:
		print 'Could not open', log_file
		return
	
	for line in read_log_gen(f):
		if line:
			print line
	
	f.close()        

if __name__ == '__main__':

	parser = optparse.OptionParser('usage: python ' + sys.argv[0] + ' [options] <input file>')
			 
	(_, args) = parser.parse_args()
	if len(args) < 1:
		print parser.usage
		print 'args', args
		print 'At least one file name must be supplied'
		print '--help option for more informaton'
		exit()

	# log_file is log file. It is read as text. 
	log_file = args[0]
	
	show_non_blank_lines(args[0])

	   

  
    
    