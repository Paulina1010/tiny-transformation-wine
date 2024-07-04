from collections import namedtuple
import csv
import sqlite3

con = sqlite3.connect("database") 

def convert(row):
    return row._replace(
        Rating=float(row.Rating),
        NumberOfRatings=int(row.NumberOfRatings),
        Price=float(row.Price),
    )
con.execute("DROP TABLE Wine")

#Add all wines to the Wine table into database
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
            pass
            #print(*row)

#Add Varieties to the database with ID
with open('Varieties.csv', newline='') as csvfile:
    reader = csv.reader(csvfile, delimiter=',')
    header = next(reader)
    Row = namedtuple('Row', header)
    con.execute("CREATE TABLE IF NOT EXISTS Varieties(%s, %s)" % (*header, "Id"))
    for idx, row in enumerate(map(Row._make, reader)):
        con.execute("INSERT INTO Varieties VALUES(?, ?)", (*row, idx+1))
    cur = con.execute("SELECT * FROM Varieties")
    for row in cur:
        pass
        #print(*row)
 
#Assign the kind of wine to the wines
cur = con.execute("SELECT rowid, Name FROM Wine")
con.execute("ALTER TABLE Wine ADD COLUMN Variety_ID DEFAULT NULL")

for rowid, name in cur:
    cur2 = con.execute("SELECT * FROM Varieties")
    for variety, variety_id in cur2:
        if variety in name:
            con.execute("UPDATE Wine SET Variety_ID = ? WHERE Name = ?", (variety,name)) 

#Remove white spaces
list_of_columns = ('Name', 'Country', 'Region', 'Winery', 'Year')
for col in list_of_columns:
    cur = con.execute("SELECT rowid, %s FROM Wine" % col)
    for rowid, name in cur:
        new_name = name.strip()
        con.execute("UPDATE Wine SET %s = ? WHERE rowid = ?" % col, (new_name, rowid))
    
con.commit()

cur = con.execute("SELECT * FROM Wine")
for row in cur:
    print(row)



