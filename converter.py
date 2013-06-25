__author__ = 'byronyi'

import sys
import sqlite3

def parseArgv(fileName, extension='db'):
    """parse arguments list from command line arguments
    seek for .db file"""

    assert '.' in fileName
    ext = fileName.split('.')
    assert len(ext) == 2
    ext = ext[1]
    assert ext == extension
    return fileName

def tupleKey(t):
	return t[1]

assert len(sys.argv) == 4

txtFileName = parseArgv(sys.argv[1], 'sptxt')
dbFilename = parseArgv(sys.argv[2], 'db')
MaxNumPeak = int(sys.argv[3])
assert MaxNumPeak > 0

con = sqlite3.connect(dbFilename)
c = con.cursor()

sql_stmt = 'CREATE TABLE Library (PeptideName text, Mz real, Mw real, Charge integer, NumPeak integer, '
for i in range(MaxNumPeak):
        word = 'Mz' + str(i+1) + ' real, '
        word = word + 'Intensity' + str(i+1) + ' real, '
        word = word + 'Annotation' + str(i+1) + ' text, '
        sql_stmt = sql_stmt + word
        word = ''
sql_stmt = sql_stmt[:-2] + ');'
c.execute(sql_stmt)

sql_stmt = 'INSERT INTO Library VALUES (?, ?, ?, ?, ?, '
for i in range(MaxNumPeak):
    sql_stmt = sql_stmt + '?, ?, ?, '
sql_stmt = sql_stmt[:-2] + ');'

file = open(txtFileName)
name = ''
mw = 0.0
mz = 0.0
charge = 0

Peaklist = []
readingLine = False
Entry = []

for line in file:
    line = line.strip()
    if line.startswith('Name: '):

	if readingLine:		
		readingLine = False
		Entry.sort(key = tupleKey, reverse = True)
		if len(Entry) > MaxNumPeak:
			Entry = Entry[:MaxNumPeak]
		Peaklist.append(len(Entry))
		for entry in Entry:
		    Peaklist.extend(list(entry))
		extend = 3 * MaxNumPeak + 5 - len(Peaklist)
		for i in range(extend):
			Peaklist.append(None)
		c.execute(sql_stmt, tuple(Peaklist))
		Peaklist = []
		Entry = []

        name = line[len('Name: '):]
        charge = line[-1:]
        charge = int(charge)
        Peaklist.append(name);
        Peaklist.append(charge);
    elif line.startswith('MW: '):
        mw = line[len('MW: '):]
        mw = float(mw)
        Peaklist.append(mw)
    elif line.startswith('PrecursorMZ:'):
        mz = line[len('PrecursorMZ: '):]
        mz = float(mz)
        Peaklist.append(mz)
    elif line.startswith('NumPeaks:'):
        readingLine = True
    elif readingLine and len(line.split('\t')) == 4:
        line = line.split('\t')
	line[0] = float(line[0])
	line[1] = float(line[1])
	line = tuple(line[:-1])
	Entry.append(line)

c.execute("CREATE INDEX Mz_idx on Library (Mz);")
c.close()
con.commit()


