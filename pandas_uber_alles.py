# -*- coding: utf-8 -*-
"""
    Convert ALL server logs in PaperCut bugs reports to pandas
    
"""
from __future__ import division
import re, sys, glob, os, pprint, shutil
import multiprocessing as mp
from collections import defaultdict

from common import ObjectDirectory, versions
import load_logs
import preprocess_logs

PPRINT = pprint.PrettyPrinter(indent=4)

def pprint(obj):
    PPRINT.pprint(obj)

RE_BUG_ID = re.compile(r'[A-Z]{3}-\d{3}-\d{5}')
  
def get_ids_dirs_logs(top_dir, min_logs):    
    print [root for root, _, _ in os.walk(top_dir)]
    #exit()
    
    dirs_logs = { root: sorted(glob.glob(os.path.join(root, 'server.log*'))) 
                    for root, _, _ in os.walk(top_dir) }
    dirs = sorted(dirs_logs.keys(), key=lambda x: (x.upper(), x))
    dirs = [dir for dir in dirs if len(dirs_logs[dir]) >= min_logs]
    ids_dirs = defaultdict(list)
    for dir in dirs:
        m = RE_BUG_ID.search(dir)
        if not m:
            continue
        id = m.group(0)
        print '>>', id, dir
        ids_dirs[id].append(dir)
    return dict(ids_dirs), { dir: dirs_logs[dir] for dir in dirs }


def pandasarize_all(top_dir, min_logs, n_files, n_entries):
    ids_dirs, dirs_logs = get_ids_dirs_logs(top_dir, min_logs)
    
    pprint(ids_dirs)
    pprint({dir: len(logs) for dir,logs in dirs_logs.items()})
     
    for id in sorted(ids_dirs.keys()):
        for i, dir in enumerate(sorted(ids_dirs[id])):
            hdf_path = '%s.%d' % (id, i)
            directory = ObjectDirectory(hdf_path)
            print '-' * 80
            print directory.get_dir()
            shutil.rmtree(directory.get_dir())
           
            path_pattern = os.path.join(dir, 'server.log*')
            
            load_logs.load_log_pattern(hdf_path, path_pattern, n_files=n_files)
            preprocess_logs.preprocess(directory, n_entries=n_entries)

# http://stackoverflow.com/questions/10012968/fastest-way-to-process-large-files-in-python

def task(param):
    hdf_path, dir, n_files, n_entries = param
    print '=' * 80
    print param
    print hdf_path, dir
    print '$' * 80
    
    directory = ObjectDirectory(hdf_path)
    print directory.get_dir()
    
    try:
        shutil.rmtree(directory.get_dir())
    except:
        pass
   
    path_pattern = os.path.join(dir, 'server.log*')
    
    load_logs.load_log_pattern(hdf_path, path_pattern, n_files=n_files)
    preprocess_logs.preprocess(directory, n_entries=n_entries)
    
    return True

def pandasarize_all_mp(top_dir, min_logs, n_files, n_entries, n_processes):
    ids_dirs, dirs_logs = get_ids_dirs_logs(top_dir, min_logs)
    
    pprint(ids_dirs)
    pprint({dir: len(logs) for dir,logs in dirs_logs.items()})
    
    def get_jobs():
        #print '-' * 80
        #print param
        #print '-' * 80

        for id in sorted(ids_dirs.keys()):
            for i, dir in enumerate(sorted(ids_dirs[id])):
                hdf_path = '%s.%d' % (id, i)
                print hdf_path, dir
                yield hdf_path, dir

    params = [(hdf_path, dir, n_files, n_entries) 
                for hdf_path, dir in get_jobs()]     
    pprint(params)
    
    print '$$$$$$ before'
    pool = mp.Pool(n_processes)
    ok = all(pool.imap(task, params))
    print '$$$$$$ after'
    
    return ok
    

def main():
    import optparse
    
    parser = optparse.OptionParser('python %s [options]' % sys.argv[0])
    parser.add_option('-i', '--top-dir', dest='top_dir', default=None, 
            help='Top of the logs directory tree')
    parser.add_option('-m', '--min-logs', dest='min_logs', type='int', default=1, 
            help='Min number of server.log.* in directory')
    parser.add_option('-f', '--number-files', dest='n_files', type='int', default=-1, 
            help='Number of log files to process')
    parser.add_option('-e', '--number-entries', dest='n_entries', type='int', default=-1, 
            help='Number of log entries to process')   
    parser.add_option('-p', '--parallel-processes', dest='n_processes', type='int', default=0, 
            help='Number of processed')            
    options, args = parser.parse_args()

    if not options.top_dir:
        print '    Usage: %s' % parser.usage
        print __doc__
        print '    --help for more information'
        exit()
 
    if options.n_processes:
        pandasarize_all_mp(options.top_dir, options.min_logs, options.n_files, options.n_entries,
            options.n_processes)
    else:        
        pandasarize_all(options.top_dir, options.min_logs, options.n_files, options.n_entries)


if __name__ == '__main__':
    versions()  
    main()