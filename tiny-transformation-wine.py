"""
This script loads Wine data, transform them and load data to the database.
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
CREATE TABLE Wine("VarietyId", "WineHSK", "Name", "Country", "Region", "Winery", "Rating", "NumberOfRatings", "Price", "Year", "Kind");
CREATE TABLE Varieties("Variety", "VarietyId");
CREATE TABLE Country("Country", "CountryCode")
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
    return {k: v.strip() for k, v in row.items()} #nie muszę robić kopii, bo tutaj tworzę nowy słownik


#Add hashkey to Wine table
def add_hash(kind, row):
    row = row.copy()
    hash_cols = ("Name", "Country", "Region", "Year")
    to_hash = "|".join(row[x] for x in hash_cols) + "|" + kind
    row["WineHSK"] = hashlib.md5(to_hash.encode()).hexdigest()
    
    return row


def add_variety(varieties, row):
    row = row.copy()
    for variety, variety_id in varieties:
        if variety in row["Name"]:
            row["VarietyId"] = variety_id
            break
    else:
        row["VarietyId"] = None
        
    return row
    
def add_kind(row):
    row = row.copy()
    row["Kind"] = kind

    return row
 
 
con.execute("DROP TABLE Wine")
con.execute("DROP TABLE Varieties")
con.execute("DROP TABLE Country")


con.executescript(SCHEMA)
 
#Add Varieties to the database with ID
with open("Varieties.csv", newline="") as f:
    reader = csv.DictReader(f, delimiter=",")
    #con.execute(VARIETY)
    for idx, row in enumerate(reader):
        con.execute("INSERT INTO Varieties (Variety, VarietyId) VALUES(?, ?)", (row["Variety"], idx+1))
        
#Add all wines to the Wine table into database
varieties = list(con.execute("SELECT Variety, VarietyId FROM Varieties"))
for kind in KINDS:
    with open("%s.csv" % kind, newline="") as f:
        reader = csv.DictReader(f, delimiter=",")
        it = reader
        it = map(strip, it)
        it = map(parse_numbers, it)
        it = map(partial(add_hash, kind), it)
        it = map(partial(add_variety, varieties), it)
        it = map(add_kind, it)
        for row in it:
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





