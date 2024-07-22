"""
This script loads data for wines, transform it, loads it into the database and exports it to csv files.
"""

import csv
import hashlib
import os
import sqlite3
import sys
from collections import namedtuple
from functools import partial

con = sqlite3.connect("database.sqlite") 

COUNTRIES = [("IT", "Italy"), ("AT", "Austria"), ("DE", "Germany"), ("FR", "France"), ("NZ", "New Zealand"), ("CL", "Chile"), ("PT", "Portugal"), ("IL", "Israel"), ("ZA", "South Africa"), ("ES", "Spain"), ("LU", "Luxembourg")]

SCHEMA = """
CREATE TABLE Wine(
      VarietyId INTEGER
    , WineHSK TEXT
    , Name TEXT
    , CountryName TEXT
    , Region TEXT
    , Winery TEXT
    , Rating REAL
    , NumberOfRatings INTEGER
    , Price REAL
    , Year TEXT
    , Kind TEXT
);
CREATE TABLE Variety(
      VarietyId INTEGER
    , VarietyName TEXT
);
CREATE TABLE Country(
      CountryCode TEXT
    , CountryName TEXT
)
"""

def parse_numbers(row):
    row = row.copy()
    row["Rating"] = float(row["Rating"])
    row["NumberOfRatings"] = int(row["NumberOfRatings"])
    row["Price"] = float(row["Price"])
    
    return row


def strip(row):
    """Remove whitespaces"""
    return {k: v.strip() for k, v in row.items()}


def add_hash(kind, row):
    """Add hashkey to Wine table"""
    row = row.copy()
    hash_cols = ("Name", "Country", "Region", "Year")
    to_hash = "|".join(row[x] for x in hash_cols) + "|" + kind
    row["WineHSK"] = hashlib.md5(to_hash.encode()).hexdigest()
    
    return row


def add_variety(varieties, row):
    """Add VarietyId to Variety table"""
    row = row.copy()
    for variety_id, variety_name in varieties:
        if variety_name in row["Name"]:
            row["VarietyId"] = variety_id
            break
    else:
        row["VarietyId"] = None
        
    return row


def add_kind(kind, row):
    row = row.copy()
    row["Kind"] = kind

    return row
 

if sys.argv[1] == "create":
    con.executescript(SCHEMA)
    con.commit()
    print("Wine, Variety and Country tables created", file=sys.stderr)

elif sys.argv[1] == "load-meta":
    with open("Varieties.csv", newline="") as f:
        reader = csv.DictReader(f, delimiter=",")
        for idx, row in enumerate(reader):
            con.execute("INSERT INTO Variety (VarietyId, VarietyName) VALUES(?, ?)", (idx+1, row["Variety"]))
    print("Loaded %d records into the Variety table" % (idx + 1), file=sys.stderr)
    
    for idx, (country_code, country_name) in enumerate(COUNTRIES):
        con.execute("INSERT INTO Country VALUES(?, ?)", (country_code, country_name))
    con.commit()
    print("Loaded %d records into the Country table" % (idx + 1), file=sys.stderr)

elif sys.argv[1] == "load-data":
    #Add all wines to the Wine table into database
    varieties = list(con.execute("SELECT VarietyId, VarietyName FROM Variety"))
    kind = sys.argv[2].strip(".csv")
    with open(sys.argv[2], newline="") as f:
        reader = csv.DictReader(f, delimiter=",")
        it = reader
        it = map(strip, it)
        it = map(parse_numbers, it)
        it = map(partial(add_hash, kind), it)
        it = map(partial(add_variety, varieties), it)
        it = map(partial(add_kind, kind), it)
        for idx, row in enumerate(it):
            con.execute("""
                INSERT INTO Wine (
                      WineHSK, Name, CountryName, Region, Winery, Rating
                    , NumberOfRatings, Price, Year, Kind, VarietyId
                ) VALUES(
                      :WineHSK, :Name, :Country, :Region, :Winery, :Rating
                    , :NumberOfRatings , :Price, :Year, :Kind, :VarietyId
                  )
                """, row)
    con.commit()
    print("Loaded %d records into the Wine table" % (idx + 1), file=sys.stderr)

elif sys.argv[1] == "export-csv":
    cur = con.execute("SELECT CountryCode FROM Country")
    os.makedirs("Wines", exist_ok=True)

    for country_code in cur:
        cur2 = con.execute("""
                    SELECT
                          CountryCode, WineHSK, Name, Wine.CountryName, Region, Winery
                        , Rating, NumberOfRatings, Price, Year, Kind, VarietyName 
                    FROM Wine 
                        LEFT JOIN Country 
                            ON Wine.CountryName = Country.CountryName
                        LEFT JOIN Variety 
                            ON Variety.VarietyId = Wine.VarietyId 
                    WHERE CountryCode = ?
                            """, country_code)
        with open("Wines/%s.csv" % country_code, newline="", mode="w") as f:
            fieldnames = ["CountryCode", "WineHSK", "Name", "CountryName", "Region", "Winery", "Rating", "NumberOfRatings", "Price", "Year", "Kind", "VarietyName"]
            writer = csv.writer(f, delimiter=",")
            writer.writerow(fieldnames)
            for element in cur2:
                writer.writerow(element)
    con.commit()
    print("Saved wine data into csv files by countries", file=sys.stderr)


else:
    print("""
You specified an unknown operation. You can:
 1) create tables - create
 2) load data into Variety and Country tables - load-meta
 3) load data into Wine table - load-data FILE.csv
 4) export the data to the csv files - export-csv
            """, file=sys.stderr)
    sys.exit(1)