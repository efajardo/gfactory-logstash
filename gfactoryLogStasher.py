#!/usr/bin/python                                                                                                                                                                                

import re
import sys
import os

gfactory_dir = "/var/log/gwms-factory/client"

our_dir = "/var/log/gwms-factory-condorlogs"

logTypes = ['Startd', 'Master', 'Starter']


###### From glideinwms ######
import sys
STARTUP_DIR=sys.path[0]
sys.path.append(os.path.join(STARTUP_DIR,"../../.."))
from glideinwms.factory.tools.lib import gWftLogParser
##############################

def determineListofVO(initialdir):
    user_list = []
    my_list = os.listdir(initialdir)
    for item in my_list:
        user_dir = os.path.join(initialdir, item)
        if os.path.isdir(user_dir) and re.match(r"\Auser\_", item):
                user_list.append(item)
    return user_list

def createDir(directory):
    if not os.path.exists(directory):
         print "Creating dir %s" % directory
         os.makedirs(directory)
    return directory

def createVODirs(initial_creation_dir = our_dir, vo_list = []):
    for vo in vo_list:
        newdir = os.path.join(initial_creation_dir, vo)
        createDir(newdir)

def entriesPerVO(initialdir, user):
    directory = os.path.join(initialdir, user, 'glidein_gfactory_instance')
    entries = []
    my_list = os.listdir(directory)
    for item in my_list:
        entry_dir = os.path.join(directory, item)
        if os.path.isdir(entry_dir) and re.match(r"\Aentry\_", item):
            entries.append(item)
    return entries
    

def createEntriesDirs(initial_creation_dir , vo, entry_list):
    for entry in entry_list:
        newdir = os.path.join(initial_creation_dir, vo, entry)
        createDir(newdir)
    
def determineExistentStandardErrorLogs(initialdir, user, entry):
    directory = os.path.join(initialdir, user, 'glidein_gfactory_instance', entry)
    my_list = os.listdir(directory)
    file_list = []
    for item in my_list:
        file_path = os.path.join(directory, item)
        if os.path.isfile(file_path) and re.match(r"\Ajob.*.*.err", item):
            if os.path.getsize(file_path) > 0:
                file_list.append(item)
    return file_list

def listExistingDecompressedLogs(initial_creation_dir, vo, entry):
     directory = os.path.join(initial_creation_dir, vo, entry)
     existing_list = {}
     my_list = os.listdir(directory)
     for item in my_list:
          m = re.search(r"(job\.*\.*.*)\.(Master|Startd|Starter)\.log", item)
          if m != None:
              if m.group(1) != None:
                  existing_item = m.group(1) + ".err"
                  if not existing_item in existing_list:
                      existing_list[existing_item] = []
                  existing_list[existing_item].append(m.group(2))
     return existing_list
              
def createDecompressedLogs(initialdir, initial_creation_dir, user, entry, jobid, logType = 'Master'):
    stderrFile = os.path.join(initialdir, user, 'glidein_gfactory_instance', entry, jobid)
    destinationFile = os.path.join(initial_creation_dir, user, entry,jobid) + "." + logType + ".log"
    condor_log_id = 'Master'
    if logType not in logTypes:
        print "The LogType is not: Master, Starter or Startd, bailing"
        return
    condor_log_id = condor_log_id + "Log"
    log = gWftLogParser.get_CondorLog(stderrFile, condor_log_id)
    file = open(destinationFile,"w")
    file.write(log)
    file.close()
        

    
print "Hello World"
vo_list = determineListofVO(gfactory_dir)
createVODirs(our_dir, vo_list)
for vo in vo_list:
    print "Entries exist for vo: %s" % vo
    entry_list = entriesPerVO(gfactory_dir, vo)
    print "Creating Entry dirs"
    createEntriesDirs(our_dir, vo, entry_list)
    for entry in entry_list:
        existent_files_list = determineExistentStandardErrorLogs(gfactory_dir, vo, entry)
        print "Number of current .err pilot files for entry:%s is %d" % (entry, len(existent_files_list))
        existent_decompressed_list = listExistingDecompressedLogs(our_dir, vo, entry)
        print "Existing decompressed list size %d" % len(existent_decompressed_list)
        for file_err in existent_files_list:
            if file_err not in  existent_decompressed_list:
                for logType in logTypes:
                    createDecompressedLogs(gfactory_dir, our_dir, vo, entry, file_err, logType)

                
            
    
    
