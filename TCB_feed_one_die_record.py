import pandas as pd
import csv
import os
import re
import datetime

# developed by Wenkaihu, 2022/06

# regular express set as logre for match log file.
logre = "^TCB\d+OpLog\d+.{1,}log"

# Primary Address for remote folder. contains all links sub folders.
root = r"\\t7filer04\toollog\Logs\A48\TCB-OpLog"
rootDir = r"defined"
# define for single log file reading and Specify Data extract

writeinrows = pd.DataFrame()
nowtime = datetime.datetime.now()
# print(nowtime.timestamp())
# only extract data from recent 10 days, will release more server time
comparetime = datetime.datetime.now() - datetime.timedelta(days=10)
# print(comparetime)

# this is for reading file specific content
def extract(logfile, rootDir):
    global writeinrows
    ff = open(logfile, encoding="gb18030", errors="ignore")
    dataset = pd.read_csv(ff, sep='\t', )
    # select content aligned with "XX"
    die_drop_data = dataset[dataset['Title'] == 'Feed One Die']
    # seek for entity info from file path
    link = re.findall(r'(?<=\\)\w{6}$', rootDir)
    # print(link)
    die_drop_data.insert(loc=10, column='entity', value=link[0])
    # print(die_drop_data)
    mid = pd.concat([writeinrows, die_drop_data], ignore_index=True)
    writeinrows = mid


# Traversal of all files and calls the function.
def listDir(rootDir):
    for filename in os.listdir(rootDir):
        pathname = os.path.join(rootDir, filename)
        if os.path.isfile(pathname):
            # print(re.findall(r'^TCB\d+OpLog\d+.{1,}log',filename))
            oplog = re.findall(r'^TCB\d+OpLog\d+.+log', filename)

            # only read the file in a fixed name
            if oplog:
                filetime = os.path.getmtime(pathname)
                # only read files created less than X days, will save more running time
                if filetime > comparetime.timestamp():
                    # comparetime.timestamp():
                    print(filetime)
                    extract(pathname, rootDir)
        else:
            listDir(pathname)


listDir(root)
print(writeinrows)

# create file and write in header
f = open('TCB_feed_one_die_record.csv', 'w', newline='')
csv_writer = csv.writer(f)

writeinrows.to_csv('TCB_feed_one_die_record.csv', mode='a', index=False, header=True)

