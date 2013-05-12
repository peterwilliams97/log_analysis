import re

txt='2011-01-24 12:15:09'

re1='((?:2|1)\\d{3}(?:-|\\/)(?:(?:0[1-9])|(?:1[0-2]))(?:-|\\/)(?:(?:0[1-9])|(?:[1-2][0-9])|(?:3[0-1]))(?:T|\\s)(?:(?:[0-1][0-9])|(?:2[0-3])):(?:[0-5][0-9]):(?:[0-5][0-9]))'	# Time Stamp 1

rg = re.compile(re1,re.IGNORECASE|re.DOTALL)
m = rg.search(txt)
if m:
    timestamp1=m.group(1)
    print "("+timestamp1+")"+"\n"
