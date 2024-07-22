"""
This script loads Wine data, transform them and load data to the database.
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

KINDS = ("White", "Red", "Rose", "Sparkling") 

SCHEMA = """
CREATE TABLE Wine(
      VarietyId INTEGER
    , WineHSK VARCHAR(32)
    , Name VARCHAR(255)
    , CountryName VARCHAR(20)
    , Region VARCHAR(50)
    , Winery VARCHAR(50)
    , Rating REAL
    , NumberOfRatings INTEGER
    , Price REAL
    , Year VARCHAR(4)
    , Kind VARCHAR(20)
);
CREATE TABLE Variety(
      VarietyId INTEGER
    , VarietyName VARCHAR(50)
);
CREATE TABLE Country(
      CountryCode VARCHAR(2)
    , CountryName VARCHAR(20)
)
"""

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
    for variety_id, variety_name in varieties:
        if variety_name in row["Name"]:
            row["VarietyId"] = variety_id
            break
    else:
        row["VarietyId"] = None
        
    return row
    
def add_kind(row):
    row = row.copy()
    row["Kind"] = kind

    return row
 
#CREATE

if sys.argv[1] == "create":
    con.executescript(SCHEMA)
    con.commit()
    print("Utworzono tabele Wine, Variety i Country", file=sys.stderr)


#LOAD

elif sys.argv[1] == "load":
    
    #Add Variety to the database with ID
    with open("Varieties.csv", newline="") as f:
        reader = csv.DictReader(f, delimiter=",")
        n_var = 0
        for idx, row in enumerate(reader):
            con.execute("INSERT INTO Variety (VarietyId, VarietyName) VALUES(?, ?)", (idx+1, row["Variety"]))
            n_var += 1
            
    #Add Country table
    n_cou = 0
    for country_code, country_name in COUNTRIES:
        con.execute("INSERT INTO Country VALUES(?, ?)", (country_code, country_name))
        n_cou += 1
    con.commit()
   
    print("Załadowano %d rekordów do tabeli Variety" % n_var, file=sys.stderr)
    print("Załadowano %d rekordów do tabeli Country" % n_cou, file=sys.stderr)


#LOAD WINES

elif sys.argv[1] == "load_wine":
    #Add all wines to the Wine table into database
    #print(sys.argv[2])
    varieties = list(con.execute("SELECT VarietyId, VarietyName FROM Variety"))
    n_win = 0
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
                con.execute("INSERT INTO Wine (WineHSK, Name, CountryName, Region, Winery, Rating, NumberOfRatings, Price, Year, Kind, VarietyId) VALUES(:WineHSK, :Name, :Country, :Region, :Winery, :Rating, :NumberOfRatings, :Price, :Year, :Kind, :VarietyId)", row)
                n_win += 1
    con.commit()
    print("Załadowano %d rekordów do tabeli Wine" % n_win, file=sys.stderr)

#EXPORT CSV

elif sys.argv[1] == "export_csv":
    #Save data to csv grouping by country
    cur = con.execute("SELECT CountryCode FROM Country")
    os.makedirs("Wines", exist_ok=True)

    for country_code in cur:
        cur2 = con.execute("SELECT CountryCode, WineHSK, Name, Wine.CountryName, Region, Winery, Rating, NumberOfRatings, Price, Year, Kind, VarietyName FROM Wine LEFT JOIN Country ON Wine.CountryName = Country.CountryName LEFT JOIN Variety ON Variety.VarietyId = Wine.VarietyId WHERE CountryCode = ?", country_code)
        with open("Wines/%s.csv" % country_code, newline="", mode="w") as f:
            fieldnames = ["CountryCode", "WineHSK", "Name", "CountryName", "Region", "Winery", "Rating", "NumberOfRatings", "Price", "Year", "Kind", "VarietyName"]
            writer = csv.writer(f, delimiter=",")
            writer.writerow(fieldnames)
            for element in cur2:
                writer.writerow(element)
    con.commit()
    print("Zapisano dane o winach do plików csv w podziale na poszczególne kraje", file=sys.stderr)


else:
    print("""
Podałeś nieznaną operację. Możesz:
 1) utworzyć tabele - create
 2) załadować dane do tabel Variety oraz Country - load
 3) załadować dane do tabeli Wine - load_wine
 4) wyeksportować dane do pliku csv - export_csv
            """, file=sys.stderr)
    sys.exit(1)







