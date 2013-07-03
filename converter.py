__author__ = 'byronyi'

import sys
import sqlite3


def parseArgv(fileName, extension='db'):
    """parse arguments list from command line arguments
    :rtype : string fileName
    seek for .db file"""

    assert '.' in fileName
    ext = fileName.split('.')
    ext = ext[-1]
    assert ext == extension
    return fileName


def tupleKey(t):
    return t[1]


assert len(sys.argv) == 3
txtFileName = parseArgv(sys.argv[1], 'sptxt')
dbFilename = parseArgv(sys.argv[2], 'db')

con = sqlite3.connect(dbFilename)
c = con.cursor()

peptide_stmt = 'CREATE TABLE Peptide (LibId integer, PeptideName text, Charge integer, PrecursorMz real, Comment text, NumPeak integer)'
c.execute(peptide_stmt)

peaklist_stmt = 'CREATE TABLE Peaklist (LibID int, Mz real, Intensity real, Annotation text, IntensityRank integer)'
c.execute(peaklist_stmt)

peptide_stmt = 'INSERT INTO Peptide VALUES (?, ?, ?, ?, ?, ?)'
peaklist_stmt = 'INSERT INTO Peaklist VALUES (?, ?, ?, ?, ?)'

file = open(txtFileName)
name = ''
charge = 0
id = 0
mz = 0.0
comment = ''
numPeaks = 0
peaklist = []

for line in file:
    line = line.strip()
    if line.startswith('Name: '):
        name = line[len('Name: '):]
        charge = line[-1:]
        charge = int(charge)
    elif line.startswith('LibID: '):
        id = line[len('LibID: '):]
        id = int(id)
    elif line.startswith('PrecursorMZ:'):
        precursormz = line[len('PrecursorMZ: '):]
        precursormz = float(precursormz)
    elif line.startswith('Comment:'):
        comment = line[len('Comment: '):]        
    elif line.startswith('NumPeaks:'):
        numPeaks = line[len('NumPeaks: '):]
        numPeaks = int(numPeaks)
    elif len(line.split('\t')) >= 3:
        line = line.split('\t')
        mz = float(line[0])
        intensity = float(line[1])	
	peak = (mz, intensity, line[2])
	peaklist.append(peak)
    elif line == '':
	peaklist.sort(key = tupleKey, reverse = True)
	for i in range(len(peaklist)):
		peak = (id,) + peaklist[i] + (i+1,)
		c.execute(peaklist_stmt, peak)

	peaklist = []
        entry = (id, name, charge, precursormz, comment, numPeaks)
        c.execute(peptide_stmt, entry)

c.execute("CREATE INDEX Mz_idx on Peptide (PrecursorMz);")
c.execute("CREATE INDEX IntensityRank_idx on Peaklist (LibID, IntensityRank)")
c.execute("CREATE VIEW Peaklist_50 as SELECT LibID, Mz, Intensity, Annotation FROM Peaklist WHERE IntensityRank <= 50")
c.execute("CREATE VIEW Peaklist_75 as SELECT LibID, Mz, Intensity, Annotation FROM Peaklist WHERE IntensityRank <= 75")
c.execute("CREATE VIEW Peaklist_100 as SELECT LibID, Mz, Intensity, Annotation FROM Peaklist WHERE IntensityRank <= 100")
c.execute("CREATE VIEW Peaklist_150 as SELECT LibID, Mz, Intensity, Annotation FROM Peaklist WHERE IntensityRank <= 150")


c.close()
con.commit()
