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
CREATE TABLE IF NOT EXISTS Wine("VarietyId", "WineHSK", "Name", "Country", "Region", "Winery", "Rating", "NumberOfRatings", "Price", "Year", "Kind");
CREATE TABLE IF NOT EXISTS Varieties("Variety", "VarietyId");
CREATE TABLE IF NOT EXISTS Country("Country", "CountryCode")
"""

con = sqlite3.connect("database.sqlite") 

def parse_numbers(row):
    row = row.copy() #tworze kopie słownika, zeby nie dzialac na oryginalnych danych. Dzialanie na oryginalnych danych powoduje bugi; mozna tez zapisac row = dict(row) 
    row["Rating"] = float(row["Rating"])
    row["NumberOfRatings"] = int(row["NumberOfRatings"])
    row["Price"] = float(row["Price"])
    return row

#Remove whitespaces
def strip(row):
    row = row.copy()
    return {k: v.strip() for k, v in row.items()}

#Add hashkey to Wine table
def hash_(kind, row):
    row = row.copy()
    to_hash = ("Name", "Country", "Region", "Year")
    row_new = "|".join(row[x] for x in to_hash) + "|" + kind
    row["WineHSK"] = hashlib.md5(row_new.encode()).hexdigest()
    return row

def variety(row):
    row = row.copy()
    varieties = list(con.execute("SELECT Variety, VarietyId FROM Varieties"))
    for variety, VarietyId in varieties:
        if variety in row["Name"]:
            row["VarietyId"] = VarietyId
            break
    else:
        row["VarietyId"] = None
    return row
 
con.execute("DROP TABLE Wine")
con.execute("DROP TABLE Varieties")

#CREATE
con.executescript(SCHEMA)
 
#Add Varieties to the database with ID
with open("Varieties.csv", newline="") as f:
    reader = csv.DictReader(f, delimiter=",")
    #con.execute(VARIETY)
    for idx, row in enumerate(reader):
        con.execute("INSERT INTO Varieties (Variety, VarietyId) VALUES(?, ?)", (row["Variety"], idx+1))
        
#Add all wines to the Wine table into database
for kind in KINDS:
    with open("%s.csv" % kind, newline="") as f:
        reader = csv.DictReader(f, delimiter=",")
        it = reader
        it = map(strip, it)
        it = map(parse_numbers, it)
        it = map(partial(hash_, kind), it)
        it = map(variety, it)
        for row in it:
            row["Kind"] = kind
            con.execute("INSERT INTO Wine (WineHSK, Name, Country, Region, Winery, Rating, NumberOfRatings, Price, Year, Kind, VarietyId) VALUES(:WineHSK, :Name, :Country, :Region, :Winery, :Rating, :NumberOfRatings, :Price, :Year, :Kind, :VarietyId)", row)

#Add Country table
for country, countrycode in COUNTRIES:
    con.execute("INSERT INTO Country VALUES(?, ?)", (country, countrycode))

#Save data to csv grouping by country
cur = con.execute("SELECT CountryCode FROM Country")
os.makedirs("Wines", exist_ok=True)

for countrycode in cur:
    cur2 = con.execute("SELECT CountryCode, WineHSK, Name, Wine.Country, Region, Winery, Rating, NumberOfRatings, Price, Year, Kind, Variety FROM Wine LEFT JOIN Country ON Wine.Country = Country.Country LEFT JOIN Varieties ON Varieties.VarietyId = Wine.VarietyId WHERE CountryCode = ?", countrycode)
    with open("Wines/%s.csv" % countrycode, newline="", mode="w") as f:
        fieldnames = ["CountryCode", "WineHSK", "Name", "Country", "Region", "Winery", "Rating", "NumberOfRatings", "Price", "Year", "Kind", "Variety"]
        writer = csv.writer(f, delimiter=",")
        writer.writerow(fieldnames)
        for element in cur2:
            writer.writerow(element)
con.commit()





