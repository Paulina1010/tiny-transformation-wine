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
 
con.commit()
cur = con.execute("SELECT rowid, Name FROM Wine")
con.execute("ALTER TABLE Wine ADD COLUMN Variety_ID DEFAULT NULL")

for rowid, name in cur:
    assert name is not None
    #print(name)
    cur2 = con.execute("SELECT * FROM Varieties")
    for variety, variety_id in cur2:
        if variety in name:
            con.execute("INSERT INTO Wine(Variety_ID) VALUES(?)", (variety_id,)) 
            # tu jest b��d, bo nie chodzi o to, �eby doda� kolejny rekord
            # tylko doda� warto�� do aktualnego rekordu
            
cur = con.execute("SELECT * FROM Wine")
for row in cur:
    print(row)



'''
 def vlookup(basic_table, search_table):
    for value in basic_table[0]:
      for search_value in search_table[0]:
            if(value==search_value):
                return_value = con.execute("SELECT Id FROM Varieties")
                con.execute("INSERT INTO Wine (Variety_ID) VALUES(?)", (return_value))
'''

    
 
#con.execute("ALTER TABLE Wine ADD COLUMN Variety_ID DEFAULT NULL")