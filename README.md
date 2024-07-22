# Tiny Transformation Wine

Tiny Transformation Wine is a wine data transformation project. 
Data was extracted from [kagle.com] (https://www.kaggle.com/datasets/budnyak/wine-rating-and-price), transformed, stored in a SQLITE database and exported to CSV files for each country.

The program is divided into several commands:
1. create
    - create tables in the SQLITE database
    - specify column types
2. load-meta
    - loading data into the Variety table from a csv file and adding VarietyId
    - loading data into the Country table, which were defined in the program
3. load-data
    - loading wine data, i.e. white, red, rose and sparkling wines
    - execution of the strip function, which removes whitespaces
    - creating the HSK
    - assigning Variety based on the wine name
    - adding a column to specify the kind of wine
4. export-csv
    - exporting data to files by country

## Usage   
You need to have python downloaded to run the code.

To run the program, download this program and data from [kagle.com] (https://www.kaggle.com/datasets/budnyak/wine-rating-and-price), put them in the same folder and run the script as presented below.

```sh
# create tables
python3 tiny-transformation-wine.py create
 
# load metadata
python3 tiny-transformation-wine.py load-meta

# load data for White wines
python3 tiny-transformation-wine.py load-data White.csv

# load data for Red wines
python3 tiny-transformation-wine.py load-data Red.csv

# load data for Rose wines
python3 tiny-transformation-wine.py load-data Rose.csv

# load data for Sparkling wines
python3 tiny-transformation-wine.py load-data Sparkling.csv

# export the data to the csv files
python3 tiny-transformation-wine.py export-csv
```
The final result of the program is the creation of the Wines folder, which contains CSV files with wines, for each country.

## Contributing
This project was done as part of a data engineering skills exercise, it is fully working, but if anyone would like to improve this project then they can send a pull request.

Report bugs on the [issue tracker](https://github.com/Paulina1010/programy-python/issues).

## License
MIT, see[LICENSE](LICENSE)