from collections import namedtuple
import csv

def convert(row):
    return row._replace(
        Rating=float(row.Rating),
        NumberOfRatings=int(row.NumberOfRatings),
        Price=float(row.Price),
    )

with open('white.csv', newline='') as csvfile:
    spamreader = csv.reader(csvfile, delimiter=',')
    Row = namedtuple('Row', next(spamreader))
    print(Row)

    for row in map(convert, map(Row._make, spamreader)):
        for x, colname in zip(row, Row._fields):
            print(colname, x, type(x).__name__, end=' | ')
        print()
        print()