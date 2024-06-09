from collections import namedtuple
import csv
import sqlite3

con = sqlite3.connect(":memory:") 

def convert(row):
    return row._replace(
        Rating=float(row.Rating),
        NumberOfRatings=int(row.NumberOfRatings),
        Price=float(row.Price),
    )

kinds = ('White', 'Red', 'Rose', 'Sparkling')
    
for kind in kinds:
    with open('%s.csv' % kind, newline='') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        header = next(reader)
        Row = namedtuple('Row', header)
        con.execute("CREATE TABLE IF NOT EXISTS Wine(%s, %s, %s, %s, %s, %s, %s, %s, %s)" % (*header, "Kind"))
        for row in map(convert, map(Row._make, reader)):
            con.execute("INSERT INTO Wine VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)", (*row, kind))
        cur = con.execute("SELECT * FROM Wine")
        for row in cur:
            print(*row)

with open('Varieties.csv', newline='') as csvfile:
    reader = csv.reader(csvfile, delimiter=',')
    header = next(reader)
    Row = namedtuple('Row', header)
    con.execute("CREATE TABLE IF NOT EXISTS Varieties(%s, %s)" % (*header, "Id"))
    row_id = 1
    for row in map(Row._make, reader):
        con.execute("INSERT INTO Varieties VALUES(?, ?)", (*row, row_id))
        row_id += 1
    cur = con.execute("SELECT * FROM Varieties")
    for row in cur:
        print(*row)
 