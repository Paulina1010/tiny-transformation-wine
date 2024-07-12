"""
This script loads Wine data, transform them and load data to database.
"""

import csv
import hashlib
import os
import sqlite3
from collections import namedtuple
from functools import partial

COUNTRIES = [("Italy", "IT"), ("Austria", "AT"), ("Germany", "DE"), ("France", "FR"), ("New Zealand", "NZ"), ("Chile", "CL"), ("Portugal", "PT"), ("Israel", "IL"), ("South Africa", "ZA"), ("Spain", "ES"), ("Luxembourg", "LU")]

KINDS = ("White", "Red", "Rose", "Sparkling") 
 
SCHEMA = """
CREATE TABLE IF NOT EXISTS Wine("WINE_HSK", "Name", "Country", "Region", "Winery", "Rating", "NumberOfRatings", "Price", "Year", "Kind")
"""

con = sqlite3.connect("database.sqlite") 

def parse_numbers(row):
    return row._replace(
        Rating=float(row.Rating),
        NumberOfRatings=int(row.NumberOfRatings),
        Price=float(row.Price),
    )

#Remove whitespaces
def strip(row):
    return tuple(x.strip() for x in row)

#Add hashkey to Wine table
def hash_(kind, row):
    #Mam namedtuple
    row_dict = row._asdict()
    #Mam dict
    to_hash = ("Name", "Country", "Region", "Year")
    row_new = "|".join(row_dict[x] for x in to_hash) + "|" + kind
    hash_key = hashlib.md5(row_new.encode()).hexdigest()
    return hash_key, *row

con.execute("DROP TABLE Wine")

#Add all wines to the Wine table into database
for kind in KINDS:
    with open("%s.csv" % kind, newline="") as f:
        reader = csv.reader(f, delimiter=",")
        header = next(reader)
        Row = namedtuple('Row', header)
        con.execute(SCHEMA)
        it = reader
        it = map(strip, it) #Remove whitespaces
        it = map(Row._make, it)
        it = map(parse_numbers, it)
        it = map(partial(hash_, kind), it)
        for row in it:
            con.execute("INSERT INTO Wine VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (*row, kind))

#Add Varieties to the database with ID
with open("Varieties.csv", newline="") as f:
    reader = csv.reader(f, delimiter=",")
    header = next(reader)
    Row = namedtuple("Row", header)
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


#Add Country table
con.execute("CREATE TABLE IF NOT EXISTS Country(Country, CountryCode)")
for country, countrycode in COUNTRIES:
    con.execute("INSERT INTO Country VALUES(?, ?)", (country, countrycode))

#Save data to csv grouping by country
cur = con.execute("SELECT CountryCode FROM Country")
os.makedirs("Wines", exist_ok=True)

for countrycode in cur:
    cur2 = con.execute("SELECT CountryCode, WINE_HSK, Name, Wine.Country, Region, Winery, Rating, NumberOfRatings, Price, Year, Kind FROM Wine LEFT JOIN Country ON Wine.Country = Country.Country WHERE CountryCode = ?", countrycode)
    with open("Wines/%s.csv" % countrycode, newline="", mode="w") as f:
        fieldnames = ["CountryCode", "WINE_HSK", "Name", "Country", "Region", "Winery", "Rating", "NumberOfRatings", "Price", "Year", "Kind"]
        writer = csv.writer(f, delimiter=",")
        writer.writerow(fieldnames)
        for element in cur2:
            writer.writerow(element)
    
con.commit()





