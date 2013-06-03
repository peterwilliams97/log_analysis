# -*- coding: utf-8 -*-
"""
    Convert ALL server logs in PaperCut bugs reports to pandas
    
"""
from __future__ import division
import re, sys, glob, os, pprint, shutil
import multiprocessing as mp
from collections import OrderedDict

from common import ObjectDirectory, versions
import load_logs
import preprocess_logs

PPRINT = pprint.PrettyPrinter(indent=4)

def pprint(obj):
    PPRINT.pprint(obj)

    
RE_BUG_ID = re.compile(r'[A-Z]{3}-\d{3}-\d{5}')
  
def get_ids_dirs_logs(top_dir, min_logs):    
    import pickle
    
    print top_dir, min_logs
    
    if False and os.path.exists('dirs.pkl'):
        dirs = pickle.load(open('dirs.pkl', 'rb'))
        print 'got pickle'
    else:
        dirs = [root for root, _, _ in os.walk(top_dir)]
        #for i, dir in enumerate(dirs):
        #    print '%3d: %s' % (i, dir)
        pickle.dump(dirs, open('dirs.pkl', 'wb'))    

    print '+' * 80

    def sanitize(mask):
        mask = mask.replace('\\', '/')
        mask = mask.replace('[', r'\[')
        mask = mask.replace(']', r'\]')
        mask = mask.replace('{', r'\{')
        mask = mask.replace(']', r'\}')
        return mask

    dirs_logs = {}
    for dir in dirs:
        #print '>>', dir,
        mask = sanitize(os.path.join(dir, 'server.log*'))
        #print mask, 
        logs = sorted(glob.glob(mask))
        #print len(logs)
        if logs:
            dirs_logs[dir] = logs
            #print dir, dirs_logs[dir]
        
    dirs_logs = {dir: sorted(glob.glob(sanitize(os.path.join(dir, 'server.log*')))) 
                    for dir in dirs}
                    
    dirs = dirs_logs.keys()                
    dirs = [dir for dir in dirs if len(dirs_logs[dir]) >= min_logs]
    dirs = sorted(dirs, key=lambda k: (len(dirs_logs[k]), k.upper(), k))
    
    ids_dirs = OrderedDict()
    for dir in dirs:
        m = RE_BUG_ID.search(dir)
        if not m:
            continue
        id = m.group(0)
        # print '>>', id, dir
        ids_dirs.setdefault(id, []).append(dir)
        
    return dict(ids_dirs), {dir: dirs_logs[dir] for dir in dirs}


def get_jobs(ids_dirs):
    for id in sorted(ids_dirs.keys()):
        for i, dir in enumerate(sorted(ids_dirs[id])):
            hdf_path = '%s.%d' % (id, i)
            #print hdf_path, dir
            yield hdf_path, dir


# http://stackoverflow.com/questions/10012968/fastest-way-to-process-large-files-in-python
def process_dir(param):
    
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

    if load_logs.load_log_pattern(hdf_path, path_pattern, n_files=n_files):
        preprocess_logs.preprocess(directory, n_entries=n_entries)

    try:
        shutil.rmtree(directory.get_dir(temp=True))
    except:
        pass

    try:    
        directory.get_path('logs.h5')
    except:
        pass

    return True

 
def pandasarize_all(top_dir, min_logs, n_files, n_entries):

    ids_dirs, dirs_logs = get_ids_dirs_logs(top_dir, min_logs)

    pprint(ids_dirs)
    pprint({dir: len(logs) for dir,logs in dirs_logs.items()})

    for hdf_path, dir in get_jobs(ids_dirs):
        process_dir((hdf_path, dir, n_files, n_entries))  


def pandasarize_all_mp(top_dir, min_logs, n_files, n_entries, n_processes):

    ids_dirs, dirs_logs = get_ids_dirs_logs(top_dir, min_logs)

    pprint(ids_dirs)
    pprint({dir: len(logs) for dir,logs in dirs_logs.items()})

    params = [(hdf_path, dir, n_files, n_entries) for hdf_path, dir in get_jobs(ids_dirs)]     
    pprint(params)

    print '$$$$$$ before'
    pool = mp.Pool(n_processes)
    ok = all(pool.imap(process_dir, params))
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