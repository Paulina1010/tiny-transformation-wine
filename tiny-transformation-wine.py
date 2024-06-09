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
    

with open('white.csv', newline='') as csvfile:
    spamreader = csv.reader(csvfile, delimiter=',')
    header = next(spamreader)
    Row = namedtuple('Row', header)
    con.execute("CREATE TABLE IF NOT EXISTS wine(%s, %s, %s, %s, %s, %s, %s, %s, %s)" % (*header, "kind"))
    for row in map(convert, map(Row._make, spamreader)):
        con.execute("INSERT INTO wine VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)", (*row, "white"))
    cur = con.execute("SELECT * FROM wine")
    for row in cur:
        print(*row)

        