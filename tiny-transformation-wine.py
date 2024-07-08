from collections import namedtuple
import csv
import sqlite3
import hashlib
import os

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
            con.execute("UPDATE Wine SET Variety_ID = ? WHERE Name = ?", (variety_id,name)) 

#Remove white spaces
list_of_columns = ('Name', 'Country', 'Region', 'Winery', 'Year')
for col in list_of_columns:
    cur = con.execute("SELECT rowid, %s FROM Wine" % col)
    for rowid, name in cur:
        new_name = name.strip()
        con.execute("UPDATE Wine SET %s = ? WHERE rowid = ?" % col, (new_name, rowid))
        
#Add column WINE_HSK(dla Name, country, region, year, kind).
cur = con.execute("SELECT rowid, Name, Country, Region, Year, Kind FROM Wine")
con.execute("ALTER TABLE Wine ADD COLUMN WINE_HSK DEFAULT NULL")

for row in cur:
    rowid, *to_hash = row
    row_new = "|".join(to_hash)
    hash_key = hashlib.md5(row_new.encode()).hexdigest()
    con.execute("UPDATE Wine SET WINE_HSK = ? WHERE rowid = ?", (hash_key, rowid))

#Add Country table
con.execute("CREATE TABLE IF NOT EXISTS Country(Country, CountryCode)")
list_of_countries = [("Italy", "IT"), ("Austria", "AT"), ("Germany", "DE"), ("France", "FR"), ("New Zealand", "NZ"), ("Chile", "CL"), ("Portugal", "PT"), ("Israel", "IL"), ("South Africa", "ZA"), ("Spain", "ES"), ("Luxembourg", "LU")]
for country, countrycode in list_of_countries:
    con.execute("INSERT INTO Country VALUES(?, ?)", (country, countrycode))

#Save data to csv grouping by country
cur = con.execute("SELECT CountryCode FROM Country")
os.makedirs("Wines", exist_ok=True)

for countrycode in cur:
    cur2 = con.execute("SELECT CountryCode, WINE_HSK, Name, Wine.Country, Region, Winery, Rating, NumberOfRatings, Price, Year, Kind FROM Wine LEFT JOIN Country ON Wine.Country = Country.Country WHERE CountryCode = ?", countrycode)
    with open('Wines/%s.csv' % countrycode, newline='', mode='w') as csvfile:
        fieldnames = ['CountryCode', 'WINE_HSK', 'Name', 'Country', 'Region', 'Winery', 'Rating', 'NumberOfRatings', 'Price', 'Year', 'Kind']
        writer = csv.writer(csvfile, delimiter=',')
        writer.writerow(fieldnames)
        for element in cur2:
            writer.writerow(element)
    
con.commit()





